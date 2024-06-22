# opendiv-db
JSON type database solution where collections are directories, documents are individual files contaning a json (Document) object contaning the data under 'Data'.
Docuemnts can be encripted if encryption_key is specified.

## Configuration
The project uses either a configuration file that is must be located at under the same directory as the executable or envionrment variables.

### Configuration file example
Create a file db_config.yml in the same directory as the executable.
```
encryption_key: "@dGsxvgCvTucs324REKp6kz2-v86RYR7"
db_path: "db"
cache_timeout: 600
cache_limit: 10
```

### Enviornemnt variables example

Same values as the configuration file but set as enviornemnt variables.
```
export OPENDIV_DB_ENCRYPTION_KEY=@dGsxvgCvTucs324REKp6kz2-v86RYR7
export OPENDIV_DB_PATH=db
export OPENDIV_DB_CACHE_TIMEOUT=600
```

## Database Filter

The database currently has functionality to filter based on the following field types

- JSON string - Go String
    - If a string is RFC 3339 (Go time.RFC3339Nano) formatted it can be used to filter date time
- JSON bool - Go bool
- JSON number - Go float64

## TODO
- Document version control
- Document TLS
- Add users support
    - Add user permissions
    - User rules to control access to documents and collections
        - Use tags for access
- Add REST functions
    - Make sure permissions is enforced