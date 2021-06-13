from typing import Callable, List, Type, TypeVar, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from vivarium.framework.engine import Builder
    from vivarium.framework.event import Event
    from vivarium.framework.population import SimulantData


class State:
    def __init__(self, name: str, model: 'Machine'):
        self._name = name
        self._model = model
        self._transitions: List['Transition'] = []

    @property
    def name(self) -> str:
        state_type = self.__class__.__name__.lower()
        return f'{self.model}.{state_type}.{self._name}'

    @property
    def short_name(self) -> str:
        return self._name

    @property
    def model(self) -> str:
        return self._model.name

    @property
    def transition_names(self) -> List[str]:
        return [transition.short_name for transition in self._transitions]

    def setup(self, builder: 'Builder') -> None:
        self.initial_proportion = self.get_initial_proportion(builder)

    def on_entrance(self, index: pd.Index) -> None:
        pass

    def on_exit(self, index: pd.Index) -> None:
        pass

    def get_initial_proportion(self, builder: 'Builder') -> Callable[[pd.Index], pd.Series]:
        n_states = len(self._model._states)
        proportion = 1 / n_states
        return builder.lookup.build_table(proportion)

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self._name}, model={self.model})'


# Create a type variable with State as the upper bound to allow
# static analysis tools to infer attributes of subtypes correctly.
StateType = TypeVar('StateType', bound=State)


class Transition:

    def __init__(self, input_state: StateType, output_state: StateType):
        self._input_state = input_state
        self._output_state = output_state
        self._model = None

    @property
    def name(self) -> str:
        transition_type = self.__class__.__name__.lower()
        return f'{transition_type}.{self._input_state.name}.{self._output_state.name}'

    @property
    def short_name(self) -> str:
        return f'{self._input_state.short_name}_TO_{self._output_state.short_name}'

    def setup(self, builder: 'Builder') -> None:
        pass


class Machine:

    def __init__(self, name: str):
        self._name = name
        self._states = []

    @property
    def name(self) -> str:
        machine_type = self.__class__.__name__.lower()
        return f'{machine_type}.{self._name}'

    @property
    def sub_components(self) -> List[StateType]:
        return self._states

    @property
    def state_names(self) -> List[str]:
        return [s.short_name for s in self._states]

    @property
    def transition_names(self) -> List[str]:
        return [transition.short_name for state in self._states for transition in state.transitions]

    @property
    def columns_required(self) -> List[str]:
        return []

    @property
    def columns_created(self) -> List[str]:
        return [self._name]

    def setup(self, builder: 'Builder') -> None:
        self.randomness = builder.randomness.get_stream(f'{self.name}.initial_states')
        self.population_view = builder.population.get_view(
            self.columns_required + self.columns_created
        )
        builder.population.initializes_simulants(
            self.on_initialize_simulants,
            creates_columns=self.columns_created,
            requires_columns=self.columns_required,
            requires_streams=[self.randomness.key],
        )
        builder.event.register_listener('time_step', self.on_time_step)

    def on_initialize_simulants(self, pop_data: 'SimulantData') -> None:
        state_names, weight_bins = zip(*[
            (state.short_name, state.initial_proportion(pop_data.index))
            for state in self._states
        ])
        initial_state = self.randomness.choice(pop_data.index, state_names, weight_bins)
        self.population_view.update(initial_state)

    def on_time_step(self, event: 'Event'):
        current_state = self.population_view.subview([self._name]).get(event.index)[self._name]
        for state in self._states:
            state.transition()



    def add_state(self, state_name: str, state_type: Type[StateType] = State) -> StateType:
        state = state_type(state_name, self)
        self._states.append(state)
        return state

    def __repr__(self) -> str:
        c = self.__class__.__name__
        n = self._name
        s = [s._name for s in self._states]
        return f'{c}(name={n}, states={s})'
