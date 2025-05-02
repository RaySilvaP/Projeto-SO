# How to Run the Project

## Requirements

* [Docker](https://www.docker.com/)
* [Docker Compose](https://docs.docker.com/compose/)

## Step-by-Step Instructions

### 1. Clone the Repository

Clone the project to your local machine:

```bash
git clone https://github.com/RaySilvaP/Projeto-SO.git
```

### 2. Set Up a Shared Directory

You need to define a shared directory between your machine and the containers.
To do this, update the `volumes` section in the `docker-compose.yaml` file:

```yaml
volumes:
  # host-path:container-path
  - {PathOnYourMachine}:{PathInTheContainers}
```

**Note:** The containers will only read from and write to the directory you specify.

### 3. Move Your Input File

Place the file you want to process into the shared directory you configured.

### 4. Start the Containers

Launch the containers. You can adjust the number of workers using the `--scale` option:

```bash
docker compose up --scale reducer-worker=5 --scale mapper-worker=5
```

**Note:** Changing the number of workers may improve performance but use more system resources.

### 5. Attach to the Coordinator Container

To interact with the coordinator container, attach to it:

```bash
docker attach coordinator
```

### 6. Start Processing

Once attached, type the name of the file you want to process and press Enter.
