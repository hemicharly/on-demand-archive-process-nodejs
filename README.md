## FILE-PROCESS-ON-DEMAND

#### 1. Requirements Installation

* Install docker
* Install docker-compose
* Install Makefile

#### 2. Steps to run this project

##### 2.1. Start project in mode development

* It is recommended to create aliases for `docker-compose -f docker-compose.cli.yml run --rm` with `dcli`.

To start project using docker use the commands

| COMMAND      | DESCRIPTION                     |
|--------------|---------------------------------|
| make install | Installing dependencies project |
| make start   | Execute application             |
