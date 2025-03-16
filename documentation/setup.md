
### Prerequisites

Before running the `docker-compose up -d` command, please review the [Running on Docker](###Running-on-docker) section to modify the image resources if running on a low-end environment. Take note of the System Requirements below.

You must have the latest version of Docker and docker-compose installed. Furthermore, you also must have Postman to trigger the pipeline.

### System Requirements

| Specification       | Suggested Requirements                          | Minimum Requirements (for alternative `docker-compose`) TODO |
|---------------------|-------------------------------------------------|----------------------------------------------------------|
| **CPU**             | 6 cores, 3.5 GHz or higher                      | 3 cores                                                   |
| **RAM**             | 32 GB (16 GB allocated to Docker)               | 16 GB (8 GB allocated to Docker)                          |
| **Spark Cluster**    | 3 workers, each with 2 cores and 4 GB of RAM    | 2 workers, each with 2 cores and 2.5 GB of RAM            |
| **Storage**         | 30-50 GB                                        | 30-50 GB                                                  |


### Running Project using Docker Compose

```shell
# Open a terminal (Command Prompt or PowerShell for Windows, Terminal for macOS or Linux)

# Ensure Git is installed
# Visit https://git-scm.com to download and install console Git if not already installed

# Clone the repository
git clone https://github.com/Abblix/Oidc.Server.git](https://github.com/nimbly-dev/nyctripdata_project.git

# Navigate to the project directory
cd nyctripdata_project

# Execute this command, wait until installation is complete and after finishing it will start the server.
docker-compose up -d
```

After starting the project, navigate to [http://localhost:6789/](http://localhost:6789/). This will take you to the Mage Dashboard, where you can manage the pipelines and view data processing workflows.

![NYC Tripdata Overview Page](images/documentation/nyc_tripdata_homepage.JPG)
