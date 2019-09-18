"""
=========================
The Value Pipeline System
=========================

The value pipeline system is a vital part of the ``vivarium`` infrastructure.
It allows for values that determine the behavior of individual
:term:`simulants <Simulant>` to be constructed across across multiple
:ref:`components <components_concept>`.

For more information about when and how you should use pipelines in your
simulations, see the value system :ref:`concept note <values_concept>`.

"""
from collections import defaultdict
from typing import Callable, List

from loguru import logger
import pandas as pd

from vivarium.exceptions import VivariumError
from .utilities import from_yearly


class DynamicValueError(VivariumError):
    """Indicates an improperly configured value was invoked."""
    pass


def replace_combiner(value, mutator, *args, **kwargs):
    """Replace the previous pipeline output with the output of the mutator.

    This is the default combiner.

    """
    args = list(args) + [value]
    return mutator(*args, **kwargs)


def set_combiner(value, mutator, *args, **kwargs):
    """Aggregates source and mutator output into a set."""
    value.add(mutator(*args, **kwargs))
    return value


def list_combiner(value, mutator, *args, **kwargs):
    """Aggregates source and mutator output into a list."""
    value.append(mutator(*args, **kwargs))
    return value


def rescale_post_processor(a, time_step):
    """Rescales annual rates to time-step appropriate rates."""
    return from_yearly(a, pd.Timedelta(time_step, unit='D'))


def joint_value_post_processor(a, _):
    """The final step in calculating joint values like disability weights.
    If the combiner is list_combiner then the effective formula is:

    .. math::

        value(args) = 1 -  \prod_{i=1}^{mutator count} 1-mutator_{i}(args)

    Parameters
    ----------
    a : List[pd.Series]
        a is a list of series, indexed on the population. Each series
        corresponds to a different value in the pipeline and each row
        in a series contains a value that applies to a specific simulant.
    """
    # if there is only one value, return the value
    if len(a) == 1:
        return a[0]

    # if there are multiple values, calculate the joint value
    product = 1
    for v in a:
        new_value = (1-v)
        product = product * new_value
    joint_value = 1 - product
    return joint_value


class Pipeline:
    """A single mutable value."""

    def __init__(self):
        self.name = None
        self.source = None
        self.mutators = []
        self.combiner = None
        self.post_processor = None
        self.manager = None
        self.configured = False

    def __call__(self, *args, skip_post_processor=False, **kwargs):
        return self._call(*args, skip_post_processor=skip_post_processor, **kwargs)

    def _call(self, *args, skip_post_processor=False, **kwargs):
        if not self.source:
            raise DynamicValueError(f"The dynamic value pipeline for {self.name} has no source. This likely means"
                                    f"you are attempting to modify a value that hasn't been created.")
        elif not self.configured:
            raise DynamicValueError(f"The dynamic value pipeline for {self.name} has a source but "
                                    f"has not been configured.  You've done a weird thing to get in this state.")

        value = self.source(*args, **kwargs)
        for mutator in self.mutators:
            value = self.combiner(value, mutator, *args, **kwargs)
        if self.post_processor and not skip_post_processor:
            return self.post_processor(value, self.manager.step_size())

        return value

    def __repr__(self):
        return f"_Pipeline({self.name})"


class ValuesManager:
    """The configuration of the dynamic values system.

    Notes
    -----
        Client code should never need to interact with this class
        except through the dynamic value and rate constructors exposed
        via the builder during the setup phase.

    """
    def __init__(self):
        self._pipelines = defaultdict(Pipeline)

    @property
    def name(self):
        return "values_manager"

    def setup(self, builder):
        self.step_size = builder.time.step_size()
        builder.event.register_listener('post_setup', self.on_post_setup)

        self.initialization_resources = builder.resource.get_resource_group('initialization')
        self.add_constraint = builder.lifecycle.add_constraint

        builder.lifecycle.add_constraint(self.register_value_producer, allow_during=['setup'])
        builder.lifecycle.add_constraint(self.register_value_modifier, allow_during=['setup'])
        builder.lifecycle.add_constraint(self.get_value, allow_during=['setup', 'post_setup', 'population_creation',
                                                                       'simulation_end', 'report'])

    def on_post_setup(self, event):
        # FIXME: This should raise an error, but can't due to downstream dependants.
        logger.debug(f"Unsourced pipelines: {[p for p, v in self._pipelines.items() if not v.source]}")

        for name, pipe in self._pipelines.items():
            dependencies = []
            if pipe.source:  # Same fixme as above.
                dependencies += [f'value_source.{name}']
            for i, m in enumerate(pipe.mutators):
                mutator_name = self._get_modifier_name(m)
                dependencies.append(f'value_modifier.{name}.{i}.{mutator_name}')
            self.initialization_resources.add_resources('value', [name], pipe._call, dependencies)

    def register_value_producer(self, value_name, source,
                                requires_columns=(), requires_values=(), requires_streams=(),
                                preferred_combiner=replace_combiner, preferred_post_processor=None):
        pipeline = self._register_value_producer(value_name, source, preferred_combiner, preferred_post_processor)

        # The resource we add here is just the pipeline source.
        # The value will depend on the source and its modifiers, and we'll
        # declare that resource at post-setup once all sources and modifiers
        # are registered.
        dependencies = self._convert_dependencies(source, requires_columns, requires_values, requires_streams)
        self.initialization_resources.add_resources('value_source', [value_name], source, dependencies)
        self.add_constraint(pipeline._call, restrict_during=['initialization', 'setup', 'post_setup'])
        return pipeline

    def _register_value_producer(self, value_name, source=None,
                                 preferred_combiner=replace_combiner, preferred_post_processor=None):
        logger.debug(f"Registering value pipeline {value_name}")
        pipeline = self._pipelines[value_name]
        pipeline.name = value_name
        pipeline.source = source
        pipeline.combiner = preferred_combiner
        pipeline.post_processor = preferred_post_processor
        pipeline.manager = self
        pipeline.configured = True
        return pipeline

    def register_value_modifier(self, value_name, modifier,
                                requires_columns=(), requires_values=(), requires_streams=()):
        modifier_name = self._get_modifier_name(modifier)
        logger.debug(f"Registering {modifier_name} as modifier to {value_name}")

        pipeline = self._pipelines[value_name]
        pipeline.mutators.append(modifier)

        name = f'{value_name}.{len(pipeline.mutators)}.{modifier_name}'
        dependencies = self._convert_dependencies(modifier, requires_columns, requires_values, requires_streams)
        self.initialization_resources.add_resources('value_modifier', [name], modifier, dependencies)

    def get_value(self, name):
        return self._pipelines[name]

    def __contains__(self, item):
        return item in self._pipelines

    def __iter__(self):
        return iter(self._pipelines)

    def keys(self):
        return self._pipelines.keys()

    def items(self):
        return self._pipelines.items()

    @staticmethod
    def _convert_dependencies(func, requires_columns, requires_values, requires_streams):
        # If declaring a pipeline as a value source or modifier, columns and
        # streams are optional since the pipeline itself will have all the
        # appropriate dependencies. In any situation, make sure we don't have
        # provide the pipeline function to source/modifier as well as
        # explicitly stating the pipeline name in 'requires_values'.
        if isinstance(func, Pipeline):
            dependencies = [f'value.{func.name}']
        else:
            dependencies = ([f'column.{name}' for name in requires_columns]
                            + [f'value.{name}' for name in requires_values]
                            + [f'stream.{name}' for name in requires_streams])
        return dependencies

    @staticmethod
    def _get_modifier_name(modifier):
        if hasattr(modifier, 'name'):  # This is Pipeline or lookup table or something similar
            modifier_name = modifier.name
        elif hasattr(modifier, '__self__'):  # This is a bound method of a component or other object
            owner = modifier.__self__
            owner_name = owner.name if hasattr(owner, 'name') else owner.__class__.__name__
            modifier_name = f'{owner_name}.{modifier.__name__}'
        elif hasattr(modifier, '__name__'):  # Some unbound function
            modifier_name = modifier.__name__
        elif hasattr(modifier, '__call__'):  # Some anonymous callable
            modifier_name = f'{modifier.__class__.name__}.__call__'
        else:  # I don't know what this is.
            raise ValueError(f'Unknown modifier type: {type(modifier)}')
        return modifier_name

    def __repr__(self):
        return "ValuesManager()"


class ValuesInterface:

    def __init__(self, value_manager: ValuesManager):
        self._value_manager = value_manager

    def register_value_producer(self,
                                value_name: str,
                                source: Callable[..., pd.DataFrame],
                                requires_columns: List[str] = (),
                                requires_values: List[str] = (),
                                requires_streams: List[str] = (),
                                preferred_combiner: Callable = replace_combiner,
                                preferred_post_processor: Callable[..., pd.DataFrame] = None) -> Callable:
        """Marks a ``Callable`` as the producer of a named value.

        Parameters
        ----------
        value_name
            The name of the new dynamic value pipeline.
        source
            A callable source for the dynamic value pipeline.
        requires_columns
            A list of the state table columns that already need to be present
            and populated in the state table before the pipeline source
            is called.
        requires_values
            A list of the value pipelines that need to be properly sourced
            before the pipeline source is called.
        requires_streams
            A list of the randomness streams that need to be properly sourced
            before the pipeline source is called.
        preferred_combiner
            A strategy for combining the source and the results of any calls
            to mutators in the pipeline. ``vivarium`` provides the strategies
            ``replace_combiner`` (the default), ``list_combiner``, and
            ``set_combiner`` which are importable from
            ``vivarium.framework.values``.  Client code may define additional
            strategies as necessary.
        preferred_post_processor
            A strategy for processing the final output of the pipeline.
            ``vivarium`` provides the strategies ``rescale_post_processor``
            and ``joint_value_post_processor`` which are importable from
            ``vivarium.framework.values``.  Client code may define additional
            strategies as necessary.

        Returns
        -------
            A callable reference to the named dynamic value pipeline.

        """
        return self._value_manager.register_value_producer(value_name, source,
                                                           requires_columns, requires_values, requires_streams,
                                                           preferred_combiner, preferred_post_processor)

    def register_rate_producer(self,
                               rate_name: str,
                               source: Callable[..., pd.DataFrame],
                               requires_columns: List[str] = (),
                               requires_values: List[str] = (),
                               requires_streams: List[str] = ()) -> Callable:
        """Marks a ``Callable`` as the producer of a named rate.

        This is a convenience wrapper around ``register_value_producer`` that
        makes sure rate data is appropriately scaled to the size of the
        simulation time step.  It is equivalent to
        ``register_value_producer(value_name, source,
        preferred_combiner=replace_combiner,
        preferred_post_processor=rescale_post_processor)``

        Parameters
        ----------
        rate_name
            The name of the new dynamic rate pipeline.
        source
            A callable source for the dynamic rate pipeline.
        requires_columns
            A list of the state table columns that already need to be present
            and populated in the state table before the pipeline source
            is called.
        requires_values
            A list of the value pipelines that need to be properly sourced
            before the pipeline source is called.
        requires_streams
            A list of the randomness streams that need to be properly sourced
            before the pipeline source is called.

        Returns
        -------
            A callable reference to the named dynamic rate pipeline.

        """
        return self.register_value_producer(rate_name, source,
                                            requires_columns, requires_values, requires_streams,
                                            preferred_post_processor=rescale_post_processor)

    def register_value_modifier(self,
                                value_name: str,
                                modifier: Callable,
                                requires_columns: List[str] = (),
                                requires_values: List[str] = (),
                                requires_streams: List[str] = (),):
        """Marks a ``Callable`` as the modifier of a named value.

        Parameters
        ----------
        value_name :
            The name of the dynamic value pipeline to be modified.
        modifier :
            A function that modifies the source of the dynamic value pipeline
            when called. If the pipeline has a ``replace_combiner``, the
            modifier should accept the same arguments as the pipeline source
            with an additional last positional argument for the results of the
            previous stage in the pipeline. For the ``list_combiner`` and
            ``set_combiner`` strategies, the pipeline modifiers should have
            the same signature as the pipeline source.
        requires_columns
            A list of the state table columns that already need to be present
            and populated in the state table before the pipeline modifier
            is called.
        requires_values
            A list of the value pipelines that need to be properly sourced
            before the pipeline modifier is called.
        requires_streams
            A list of the randomness streams that need to be properly sourced
            before the pipeline modifier is called.

        """
        self._value_manager.register_value_modifier(value_name, modifier,
                                                    requires_columns, requires_values, requires_streams)

    def get_value(self, name: str) -> Pipeline:
        """ Returns the pipeline registered with the simulation under the given
        name.

        Parameters
        ----------
        name
            Name of the pipeline to return.

        Returns
        -------
            A callable reference to the named pipeline.
        """
        return self._value_manager.get_value(name)
