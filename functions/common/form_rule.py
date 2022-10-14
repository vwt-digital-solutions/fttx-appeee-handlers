from typing import Optional
from enum import Enum, unique

from config import (
    TOPIC_ROUTE_RULES as TOPIC_ROUTE_RULE_LIST
)

from utils import get_from_path


@unique
class RuleType(Enum):
    EMPTY = (lambda x: x is None),
    EQUALS = (lambda x, y: x == y),
    BIGGER = (lambda x, y: x > y),
    SMALLER = (lambda x, y: x < y)

    def eval(self, arguments: list) -> bool:
        function, = self.value
        return function(*arguments)


class FormRule:
    def __init__(
            self,
            target: str,
            rule_type: RuleType,
            rule_type_args: list,
            invert: bool = False,
    ):
        self._target = target
        self._invert = invert
        self._rule_type = rule_type
        self._rule_type_args = rule_type_args

    def eval(self, form: dict) -> bool:
        value = get_from_path(form, self._target)
        return self._rule_type.eval([value, *self._rule_type_args]) ^ self._invert


def is_passing_rule(data: dict, rule: dict) -> bool:
    for sub_rule in rule["rule_set"]:
        if not sub_rule.eval(data):
            return False
    return True


def rule_from_dict(data: dict) -> FormRule:
    return FormRule(
        target=data["target"],
        rule_type=RuleType[data["type"].upper()],
        rule_type_args=data.get("type_args", []),
        invert=data.get("invert", False)
    )


def rule_from_dict(rule: dict) -> list:
    rule_set = list()
    for sub_rule in rule.get("rule_set", []):
        rule_set.append(
            FormRule(
                target=sub_rule["target"],
                rule_type=RuleType[sub_rule["type"].upper()],
                rule_type_args=sub_rule.get("type_args", []),
                invert=sub_rule.get("invert", False)
            )
        )

    return {
        "data": rule.get("data", {}),
        "rule_set": rule_set
    }


TOPIC_ROUTE_RULES = [rule_from_dict(rule) for rule in TOPIC_ROUTE_RULE_LIST]
