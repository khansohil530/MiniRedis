Simple Redis Client and Server written in Python for learning purpose

Key learnings:
- Networking with Sockets
- Concurrency with Gevent 
- RESP Protocol Design
- Redis commands
- Using decorator design pattern

Usage:
- to build image: 
    ```bash
    docker image build -t protoredis .
    ```
- to run container: 
    ```bash
    docker container run -d -v {path/on/host}:/data -p 8888:8888 protoredis
    ```
- server can store string, bytes, numbers, dictionaries, lists
- to use client
    ```python
    from client import Client

    c = Client()
    c.set('user', {'name':'John Doe', 
                   'city': 'LA',
                   'age': 50,
                   'pets': ['tom', 'jerry']
                   })
    c.get('user')
    ```