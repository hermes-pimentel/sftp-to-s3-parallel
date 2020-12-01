
# About

This script syncs a path and subdirectories from an SFTP server to an S3 bucket and is intended to run on AWS Glue Python Shell. 

Features:
    - Parallel execution (max 8) - You can change this.
    - Lookup new files in sftp N days back  - default 30. 
    - Checks if the file is already in s3.

# How to deploy

- Create an S3 bucket 
- Create a folder (e.g. scripts) into S3 bucket and copy script (.py) and paramiko wheel file.
- Create a secret at AWS Secrets Manager with name sftp, and value as plaintext:
  Change values accordingly.

```
    {
  "S3_BUCKET_NAME": "<bucket_name>",
  "S3_PATH": "<bucket path not starting with />",
  "FTP_HOST": "<ftp_host>",
  "FTP_PORT": "22",
  "FTP_USERNAME": "<ftp_user>",
  "FTP_PASSWORD": "<ftp_pass>",
  "FTP_PATH": "<ftp_path>",
  "DAYS_BACK": "30",
  "CHUNK_SIZE": "8388608"
}
```

DAYS_BACK - after how many days (last modificaiton date) the script stops checking if the file is already in S3.
CHUNK_SIZE - default for multipart upload 

- Create a role with permission to read/write to s3 bucket, and read he secret with name sftp.
- Create file job.json, exemple:
  Change values accordingly.
```
{
        "Name": "<JOB_NAME>",
        "Role": "<ARN_ROLE>",
        "ExecutionProperty": {
            "MaxConcurrentRuns": 1
        },
        "Command": {
            "Name": "pythonshell",
            "ScriptLocation": "s3://<SCRIPT_LOCATION>",
            "PythonVersion": "3"
        },
        "DefaultArguments": {
            "--enable-metrics": "",
            "--extra-py-files": "s3://<WHELL_PATH>/paramiko-2.7.2-py2.py3-none-any.whl",
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python"
        },
        "MaxRetries": 0,
        "AllocatedCapacity": 0,
        "Timeout": 2880,
        "MaxCapacity": 0.0625,
        "GlueVersion": "1.0"
}
````

- Create job:

```
aws glue create-job --cli-input-json file://job.json
```

- (optional) Create trigger or workflow


- (optional) Create wheel files

 ```
pip download --platform=manylinux1_x86_64 --only-binary=:all: -r requirements.txt
```


# License

This sample code is made available under a modified MIT license. See the LICENSE.txt file.

````
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
````
# Known Issues

* The application was written for demonstration purposes and not for production use.

# Reporting Bugs

If you encounter a bug, please create a new issue with as much detail as possible and steps for reproducing the bug