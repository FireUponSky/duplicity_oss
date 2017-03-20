# duplicity_oss
Aliyun oss backend of duplicity.

## Requirements

`pip install oss2 `

## Usage

1. Put `aliyunbackend.py` into `/usr/lib/python2.7/site-packages/duplicity/backends`.

2. Environments:

>```
>export ALIYUN_OSS_ENDPOINT="aliyun_oss_endpoint"
>export ALIYUN_ACCESS_ID="aliyun_access_id"
>export ALIYUN_ACCESS_KEY="aliyun_access_key"
>```

3. Backup:

```
duplicity --encrypt-sign-key $GPGKEYID $SOURCE oss+http://$BUCKETNAME/$TARGET
```

or

```
duplicity --encrypt-sign-key $GPGKEYID $SOURCE oss://$ALIYUN_OSS_ENDPOINT/$BUCKETNAME/$TARGET
```

## References:

This script is based on:

> `aliyunbackend.py` (https://yq.aliyun.com/articles/60986)
> `b2backend.py` (https://github.com/matthewbentley/duplicity_b2)
