"""
==============
Output Metrics
==============

Currently, ``vivarium`` uses the :ref:`values pipeline system <values_concept>`
to produce the results, or output metrics, from a simulation.  The metrics
The component here is a normal ``vivarium`` component whose only purpose is
to provide an empty :class:`dict` as the source of the *"Metrics"* pipeline.
It is included by default in all simulations.
"""
from vivarium import Component


class Metrics(Component):
    """This class declares a value pipeline that allows other components to store summary metrics."""

    def setup(self, builder):
        super().setup(builder)
        self.metrics = builder.value.register_value_producer(
            "metrics", source=lambda index: {}
        )
