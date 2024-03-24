# opendiv-db
JSON type database solution where collections are directories, documents are individual files contaning a json (Document) object contaning the data under 'Data'.
Docuemnts can be encripted if encryption_key is specified.
## TODO
- Add environment variables to specify config instead of config file
- Add users support
    - Add user permissions
    - User rules to 
- Add REST functions
    - Make sure permissions is enforced
- Add time comparison
- Add database caching.
    - Time to live and number of documents.
    - Max size for a document is 1 mb so if 1024 then that is 1 GB max cache size.
    - Caching should have a go routine to clean up expired cache and to clean up older cache when new is added
- Document version controll
- Document TLS