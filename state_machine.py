from typing import Type, Dict, Union, Optional

from sqlalchemy import ForeignKey, create_engine
from sqlalchemy.cyextension.collections import IdentitySet

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from constants import HOST,PASSWORD,DATABASE,USER


class Base(DeclarativeBase):
    pass


class StatesGptExcel(Base):
    __tablename__ = 'states_gpt_excel'

    id: Mapped[int] = mapped_column(primary_key=True, unique=True, nullable=False)
    state: Mapped[str] = mapped_column(unique=True, nullable=False)

    def __repr__(self) -> str:
        return f"States_gpt_excel(id={self.id!r}, state={self.state!r})"

    def to_dict(self) -> Dict[str, Union[str, int]]:
        return {
            "id": self.id,
            "state": self.state
        }


class UserStateGptExcel(Base):
    __tablename__ = 'userstate_gpt_excel'

    id: Mapped[int] = mapped_column(primary_key=True, unique=True, nullable=False)
    user_name: Mapped[str] = mapped_column(unique=True, nullable=False)
    state_id: Mapped[int] = mapped_column(ForeignKey("states_gpt_excel.id"))

    def __repr__(self) -> str:
        return f"Userstate_gpt_excel(id={self.id!r}, user_name={self.user_name!r}, state_id={self.state_id!r})"

    def to_dict(self) -> Dict[str, Union[str, int]]:
        return {
            "id": self.id,
            "user_name": self.user_name,
            "state_id": self.state_id
        }


class StateMachine:
    def __init__(self):
        self.engine = create_engine(
            f"postgresql+psycopg2://"
            f"{USER}:{PASSWORD}@"
            f"{HOST}/{DATABASE}",
            echo=True
        )
        Base.metadata.create_all(self.engine)
        self.session = Session(bind=self.engine)

    def get_user_state(self, user: str) -> Optional[Type[UserStateGptExcel]]:
        query = self.session.query(UserStateGptExcel)
        for row in query:
            if row.user_name == user:
                return row
        return None

    def set_user_state(self, state_name: str, user: str)\
            -> Type[UserStateGptExcel] | IdentitySet:
        existing_user = self.get_user_state(user=user)
        if existing_user:
            return self._update_existing_user(user=existing_user, state_name=state_name)
        else:
            return self._create_new_user_and_set_state(state_name=state_name, user=user)

    def add_state(self, state_name: str):
        if self.get_state(state_name=state_name) is None:
            new_state = StatesGptExcel(
                state=state_name
            )
            self.session.add(new_state)
            self.session.commit()
            return self.session.new
        else:
            return self.get_state(state_name=state_name)

    def delete_state(self, state_name: str) -> IdentitySet:
        state = self.get_state(state_name=state_name)
        self.session.delete(state)
        self.session.commit()
        return self.session.new

    def get_state(self, state_name: str) -> Optional[Type[StatesGptExcel]]:
        query = self.session.query(StatesGptExcel)
        for row in query:
            if row.state == state_name:
                return row
        return None

    def _update_existing_user(self, user: Type[UserStateGptExcel], state_name: str) -> IdentitySet:
        query_for_get_state_id = self.get_state(state_name=state_name)
        if not query_for_get_state_id:
            raise ValueError(f"Не удалось найти состояние {state_name}")
        user.state_id = query_for_get_state_id.id
        self.session.add(user)
        self.session.commit()
        return self.session.new

    def _create_new_user_and_set_state(self, state_name: str, user: str) -> IdentitySet:
        if self.get_state(state_name=state_name) is not None:
            new_user = UserStateGptExcel(
                user_name=user,
                state_id=self.get_state(state_name=state_name).id
            )
            self.session.add(new_user)
            self.session.commit()
            return self.session.new
        else:
            raise ValueError(f'Не удалость найти состояние {state_name}')
