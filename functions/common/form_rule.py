from enum import Enum, unique

from config import (
    EXCLUDE_RULES
)

from form_object import Form
from utils import get_from_path

RULES = list()


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


def is_form_excluded(form: Form) -> (bool, str):
    dictionary = form.to_dict()
    for rule in RULES:
        passed = True
        for sub_rule in rule["rule_set"]:
            if not sub_rule.eval(dictionary):
                passed = False
                break

        if not passed:
            alert = rule.get("alert", {})
            variables = alert.get("variables", {})
            for key, value in variables.items():
                variables[key] = get_from_path(dictionary, value)

            return True, alert.get("message", "").format(**variables)
    return False, None


for rule in EXCLUDE_RULES:
    rule_set = list()
    for sub_rule in rule.get("rule_set", []):
        rule_set.append(FormRule(
            target=sub_rule["target"],
            rule_type=RuleType[sub_rule["type"].upper()],
            rule_type_args=sub_rule.get("type_args", []),
            invert=sub_rule.get("invert", False)
        ))

    RULES.append({
        "alert": rule.get("alert", None),
        "rule_set": rule_set
    })
