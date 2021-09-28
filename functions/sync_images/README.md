# Synchronize APPEEE, ODH, and ArcGIS
This cloud function's purpose is to synchronize the APPEEE survey/form 
entries with ODH and ArcGIS. It does this by scanning all form entries stored on ODH, 
during this scan it:
- gets all attachments for a form,
- checks if these attachments have been downloaded yet (and if not: download them),
- and if any changes have been made by this cloud function: update them to ArcGIS.

Keep in mind that this is a maintenance cloud function to be used in case any issues with the
data pipeline occur.

## How To Run
Since this is a cloud function, it needs to be run from the dashboard with the "Test Function"
option.

### Function Arguments
There are a few arguments that can be given to this function:

| Field                         | Description                                                                      | Default | Required |
| :--------------------------   | :------------------------------------------------------------------------------- | :------ | :------: |
| form_storage_suffix           | Can be used to specify a sub directory. (Supports ranges)                        | None    | No       |
| form_index_range              | Range of indexes to be processed. (Handy for batches)                            | None    | No       |
| max_time_delta                | Specifies the maximum [timedelta][1] of the blobs, older blobs will be ignored.  | None    | No       |
| enable_attachment_downloading | Download missing attachments.                                                    | True    | No       |
| enable_arcgis_updating        | Send entries to ArcGIS when changed.                                             | True    | No       |
| force_arcgis_updating         | Always send entries to ArcGIS.                                                   | False   | No       |
| request_retry_options         | Options for request retry.                                                       | None    | No       |

[1]: https://docs.python.org/3/library/datetime.html#timedelta-objects

Example:
```json
{
    "form_storage_suffix": "/2021/[1-13]/21",
    "form_index_range": "0:100",
    "max_time_delta": {
      "days": 1
    },
    "enable_attachment_downloading": true,
    "enable_arcgis_updating": true,
    "force_arcgis_updating": false,
    "request_retry_options": {
        "retries": 6,
        "backoff": 10,
        "status_forcelist": [
          404, 500, 502, 503, 504
        ]
    }
}
```

### Output
| Field                              | Description                                   | Default |
| :--------------------------------- | --------------------------------------------- | :-----: |
| total_form_count                   | The total amount of forms scanned             | N/A     |
| form_with_missing_attachment_count | The amount of forms with missing attachments  | N/A     |
| missing_attachment_count           | The total amount of missing attachments       | N/A     |
| downloaded_attachment_count        | The amount of downloaded/restored attachments | N/A     |

Example:
```json
{
  "total_form_count": 0,
  "form_with_missing_attachment_count": 0,
  "missing_attachment_count": 0,
  "downloaded_attachment_count": 0
}
```
