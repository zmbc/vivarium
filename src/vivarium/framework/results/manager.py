from typing import Callable, List

import pandas as pd


class ResultsManager:

    @property
    def name(self):
        return 'results_manager'

    def add_mapping_strategy(self, *args, **kwargs):
        ...

    def add_default_grouping_columns(self, *args, **kwargs):
        ...

    def register_results_producer(self, *args, **kwargs):
        ...


class ResultsInterface:
    """Builder interface for the results management system.

    The results management system allows users to delegate results production
    to the simulation framework. This process attempts to roughly mimic the
    groupby-apply logic commonly done when manipulating :mod:`pandas`
    DataFrames. The representation of state in the simulation is complex,
    however, as it includes information both in the population state table
    as well as dynamically generated information available from the
    :class:`value pipelines <vivarium.framework.values.Pipeline>`.
    Additionally, good encapsulation of simulation logic typically has
    results production separated from the modeling code into specialized
    `Observer` components. This often highlights the need for transformations
    of the simulation state into representations that aren't needed for
    modeling, but are required for the stratification of produced results.

    To support these complexities, we first allow the user to supplement the
    simulation state with transformations of the underlying state table and
    pipeline values using :meth:`add_mapping_strategy`. For example, a public
    health simulation may want to group its summary results by 5-year age
    bins. A user could add a mapping strategy to transform the continuous
    "age" property of each of the simulants into a discrete set of age
    categories. This expanded version of the state can then be used to
    stratify the population into groups of interest for producing aggregate
    statistics.

    In general, most of this grouping is for all results being produced.  E.g.
    if we're going to stratify results in a public health simulation by age
    group and/or sex, we likely want to do it for all results and not just a
    few. These global stratifications can be registered with a call to
    :meth:`add_default_grouping_columns` where the columns in question are part
    of the expanded state table generated by the previously discussed mapping
    strategies.

    The suggested pattern for adding mapping strategies and default
    grouping columns is to centralize them in the `setup` method of a general
    `Metrics` or `Results` component. From this component you can expose
    configuration for the global stratification of results. This component
    should be fairly easy to reuse across entire categories of models.

    Individual `Observer` components (or model components if modeling and
    results production are mixed in the same component) can then declare
    strategies to produce result measures using
    :meth:`register_results_producer`. This method is intended to closely
    mirror the `apply` step of a :mod:`pandas` groupby-apply, taking a generic
    callable that operates on dataframes with the understanding that it
    will be called on data subsets generated by groupings defined by
    :meth:`add_default_grouping_columns`. For flexibility, a user can
    elect to add additional stratification or exclude some of the default
    stratification levels when registering a results producer.

    Results producers registered by users should only produce results about
    the current time step. The results system automatically accumulates the
    results across the whole lifetime of the simulation. The combination of the
    'measure' used when registering a results producer and the levels of
    the stratification groupings form a unique key for a particular result
    and calls to produce results every time step will accumulate values
    for a particular result if it is generated multiple times.  For example,
    we may want to track all the heart attacks that occur for 50-55 year old
    men in a several year simulation of cardiovascular health. Each time step,
    the results system will call the producer the user has registered to count
    heart attacks by age group and that producer will count all the heart
    attacks in that particular time step. The results system will then add
    that time step's worth of heart attacks for 50-55 year old men to its
    running total.

    """

    def __init__(self, manager: ResultsManager) -> None:
        self._manager = manager

    def add_mapping_strategy(self,
                             new_column: str,
                             mapper: Callable[[pd.Index], pd.Series]) -> None:
        """Adds a specification to map simulation state into a new column.

        This allows a user to specify an arbitrary function to generate
        new columns in the expanded state table for use in results production.

        For instance, suppose you have a column `time_of_death` that records
        the time a simulant dies as a time stamp.  In order to stratify
        results, you might map that column to a `year_of_death` column to
        group deaths by the year in which they occurred.

        Parameters
        ----------
        new_column
            The name of the new column in the expanded state table.
        mapper
            A callable that takes an index representing the entire
            population and returning a series of values indexed by
            the population index.

        """
        self._manager.add_mapping_strategy(new_column, mapper)

    def add_default_grouping_columns(self,
                                     grouping_columns: List[str]) -> None:
        """Add a list of expanded state table columns to group by for all measures.

        Generally, we want to be able to globally define the stratification for all
        results produced by a simulation. This method allows a user to provide
        a set of expanded state table columns that will be used to group the population
        for all results production strategies. These defaults can be
        overridden by individual results production strategies for special cases.

        Parameters
        ----------
            grouping_columns
                A list of names of columns in the expanded state table to use
                when grouping the population for results production.

        """
        self._manager.add_default_grouping_columns(grouping_columns)

    def add_results_production_strategy(self,
                                        measure: str,
                                        pop_filter: str = '',
                                        aggregator: Callable[[pd.DataFrame], float] = len,
                                        additional_grouping_columns: List[str] = (),
                                        excluded_grouping_columns: List[str] = (),
                                        when: str = 'collect_metrics') -> None:
        """Provide the framework with a strategy for producing a results 'measure'.

        Results production strategies operate like functions used in the
        `apply` step of a :mod:`pandas` groupby-apply.

        Parameters
        ----------
        measure
            The name of the measure to be produced by the strategy.
        pop_filter
            A filter to apply to the population before grouping and
            aggregation. Filters should be formatted so that they are
            consistent with arguments to :func:`pandas.DataFrame.query`.
        aggregator
            A callable operating on a :obj:`pandas.DataFrame` and producing
            a float value. Defaults to the length of the data, producing
            a group count.
        additional_grouping_columns
            Columns in the expanded state table to group the population
            by before applying any aggregation. These columns are added
            to any default grouping columns registered with
            :func:`ResultsInterface.add_default_grouping_columns`.
        excluded_grouping_columns
            Columns in the default grouping columns registered with
            :func:`ResultsInterface.add_default_grouping_columns` that should
            not be used in the production of this measure.
        when
            The name of the time step event when this measure should be
            produced.

        """
        self._manager.register_results_producer(
            measure,
            pop_filter,
            aggregator,
            list(additional_grouping_columns),
            list(excluded_grouping_columns),
            when
        )
