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

| Field                 | Description                                                   | Default |
| :-------------------: | ------------------------------------------------------------- | :-----: |
| form_storage_suffix   | Can be used to specify a sub directory.                       | None    |
| skip_download         | Skip the download of attachment files.                        | False   |
| force_arcgis_update   | Updates entry to ArcGIS, even when no changes have been found | False   |
| request_retry_options | Options for request retry                                     | None    |

Example:
```json
{
    "form_storage_suffix": "/2021/07/21",
    "skip_download": false,
    "force_arcgis_update": false,
    "request_retry_options": {
        "retries": 1,
        "backoff": 1,
        "status_forcelist": [
          404, 500, 502, 503, 504
        ]
    }
}
```

### Output
| Field                              | Description                                   | Default |
| :--------------------------------: | --------------------------------------------- | :-----: |
| total_form_count                   | The total amount of forms scanned             | N/A     |
| form_with_missing_attachment_count | The amount of forms with missing attachments  | N/A     |
| missing_attachment_count           | The total amount of missing attachments       | N/A     |
| downloaded_attachment_count        | The amount of downloaded/restored attachments | N/A     |

```json
{
  "total_form_count": 0,
  "form_with_missing_attachment_count": 0,
  "missing_attachment_count": 0,
  "downloaded_attachment_count": 0
}
```