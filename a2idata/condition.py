from enum import Enum
import re

class Condition:
    _data = None
    _conditions = None
    _result = None
    
    def __init__(self, data, conditions):
        self._data = data
        self._conditions = conditions
    
    def check(self, field_name, obj):
        r,s = [],[]
        for c in self._conditions:
            # Matching field_name or field_name_regex
            condition_setting = c.get("condition_setting")
            if c["field_name"] == field_name \
                    or (c.get("field_name") is None and c.get("field_name_regex") is not None and re.search(c.get("field_name_regex"), field_name)):
                field_value, block = None, None
                if obj is not None:
                    field_value = obj.get("value")
                    block = obj.get("block")
                    confidence = obj.get("confidence")
                
                if c["condition_type"] == "Required" \
                        and (obj is None or field_value is None or len(str(field_value)) == 0):
                    r.append({
                                "message": f"The required field [{field_name}] is missing.",
                                "field_name": field_name,
                                "field_value": field_value,
                                "condition_type": str(c["condition_type"]),
                                "condition_setting": condition_setting,
                                "condition_category":c["condition_category"],
                                "block": block
                            })
                elif c["condition_type"] == "ConfidenceThreshold" \
                    and c["condition_setting"] is not None and float(confidence) < float(c["condition_setting"]):
                    r.append({
                                "message": f"The field [{field_name}] confidence score {confidence} is lower than the threshold {c['condition_setting']}",
                                "field_name": field_name,
                                "field_value": field_value,
                                "condition_type": str(c["condition_type"]),
                                "condition_setting": condition_setting,
                                "condition_category":c["condition_category"],
                                "block": block
                            })
                elif field_value is not None and c["condition_type"] == "ValueRegex" and condition_setting is not None \
                        and re.search(condition_setting, str(field_value)) is None:
                    r.append({
                                "message": f"{c['description']}",
                                "field_name": field_name,
                                "field_value": field_value,
                                "condition_type": str(c["condition_type"]),
                                "condition_setting": condition_setting,
                                "condition_category":c["condition_category"],
                                "block": block
                            })
                
                # field has condition defined and sastified
                s.append(
                    {
                        "message": f"{c['description']}",
                        "field_name": field_name,
                        "field_value": field_value,
                        "condition_type": str(c["condition_type"]),
                        "condition_setting": condition_setting,
                        "condition_category":c["condition_category"],
                        "block": block
                    })
                
        return r, s
        
    def check_all(self):
        if self._data is None or self._conditions is None:
            return None
        
        broken_conditions = []
        satisfied_conditions = []
        for key, obj in self._data.items():
            value = None
            if obj is not None:
                value = obj.get("value")

            if value is not None and type(value)==str:
                value = value.replace(' ','')
            
            r, s = self.check(key, obj)
            if r and len(r) > 0:
                broken_conditions += r
            if s and len(s) > 0:
                satisfied_conditions += s


        # apply index
        idx = 0
        for r in broken_conditions:
            idx += 1
            r["index"] = idx
        return broken_conditions, satisfied_conditions