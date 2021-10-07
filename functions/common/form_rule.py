from typing import Optional
from enum import Enum, unique

from config import (
    EXCLUDE_RULES as EXCLUDE_RULES_DICT
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


def is_passing_rules(data: dict, rules: list) -> (bool, Optional[str]):
    for rule in rules:
        failed = False
        for sub_rule in rule["rule_set"]:
            if not sub_rule.eval(data):
                failed = True
                break

        if not failed:
            alert = rule.get("alert", {})
            if not alert:
                return True, None
            variables = alert.get("variables", {})
            parsed_variables = {}
            for key, value in variables.items():
                parsed_variables[key] = get_from_path(data, value)

            return True, alert.get("message", "").format(**parsed_variables)
    return False, None


def is_passing_exclude_rules(data: dict) -> (bool, Optional[str]):
    return is_passing_rules(data, EXCLUDE_RULES)


def rule_from_dict(data: dict) -> FormRule:
    return FormRule(
        target=data["target"],
        rule_type=RuleType[data["type"].upper()],
        rule_type_args=data.get("type_args", []),
        invert=data.get("invert", False)
    )


def rule_alerts_from_dict(data: dict) -> list:
    rules = []
    for rule in data:
        rule_set = list()
        for sub_rule in rule.get("rule_set", []):
            rule_set.append(rule_from_dict(sub_rule))

        rules.append({
            "alert": rule.get("alert", None),
            "rule_set": rule_set
        })
    return rules


EXCLUDE_RULES = rule_alerts_from_dict(EXCLUDE_RULES_DICT)
