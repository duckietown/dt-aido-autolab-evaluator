# dt-aido-autolab-evaluator

### To build
`docker-compose build`

### To run
`docker-compose up -d`
* TODO: add commands/scripts to launchers/default.sh

### To run the python pipeline
(in a running container, in `packages` folder)
Run: `python3 -m aido-autolab-evaluator.pipeline`


---
To run, remember to enable docker sock mounting:
* https://jpetazzo.github.io/2015/09/03/do-not-use-docker-in-docker-for-ci/
* Example with the base: `docker run -it --name dt-challenges-runner --net host -v /var/run/docker.sock:/var/run/docker.sock duckietown/duckietown-challenges-runner:daffy-amd64 bash`
