# opendiv-db
JSON type database solution where collections are directories, documents are individual files contaning a json (Document) object contaning the data under 'Data'.
Docuemnts can be encripted if encryption_key is specified.

## Configuration
The project uses either a configuration file that is must be located at under the same directory as the executable or envionrment variables.

### Configuration file example
Create a file db_config.json in the same directory as the executable.
```
{
    "encryption_key": "@dGsxvgCvTucs324REKp6kz2-v86RYR7",
    "db_path": "db",
    "cache_timeout": 600
}
```

### Enviornemnt variables example

Same values as the configuration file but set as enviornemnt variables.
```
export OPENDIV_DB_ENCRYPTION_KEY=@dGsxvgCvTucs324REKp6kz2-v86RYR7
export OPENDIV_DB_PATH=db
export OPENDIV_DB_CACHE_TIMEOUT=600
```

## TODO
- Add cache document limit (1 document max size 1MB)
- Add users support
    - Add user permissions
    - User rules to
- Add REST functions
    - Make sure permissions is enforced
- Add time comparison
- Document version controll
- Document TLS