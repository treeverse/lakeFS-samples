#!/bin/sh

echo -e 'server:\n    endpoint_url: http://lakefs:8000/api/v1/\ncredentials:\n    access_key_id: AKIAIOSFODNN7EXAMPLE\n    secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' > /home/lakefs/.lakectl.yaml
lakectl repo create lakefs://example-repo local://example-repo
