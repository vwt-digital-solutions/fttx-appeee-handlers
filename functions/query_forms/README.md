# Query APPEEE Forms
This Google Cloud function is used to filter through APPEEE forms.

# Arguments
| Field                         | Description                                                                      | Default | Required |
| :--------------------------   | :------------------------------------------------------------------------------- | :------ | :------: |
| form_storage_suffix           | Can be used to specify a sub directory. (Supports ranges)                        | None    | No       |
| query                         | The rule objects to match for a form to match the query.                         | None    | Yes      |

Example:
```json
{
    "form_storage_suffix": "/2021/[1-13]/01",
    "query": [
        {
            "alert": {
                "message": "Found form: {\"key\": \"{key}\"}",
                "variables": {
                    "key": "path/to/key"
                }
            },
            "rule_set": [
                {
                    "target": "path/to/key",
                    "type": "equals",
                    "type_args": ["some_key"]
                }
            ]
        }
    ]
}
```