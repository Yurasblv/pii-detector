GIT_SHA := $(shell git rev-parse --short=8 HEAD)
REGISTRY_USER ?= $(CI_REGISTRY_USER)
REGISTRY_PASSWORD ?= $(CI_REGISTRY_PASSWORD)
REGISTRY ?= $(CI_REGISTRY)
CONTAINER_IMAGE_NAME ?= $(REGISTRY)/ariksa/vulnerability/data-scannner/worker
GIT_USERNAME ?= $(GITLABPACKAGE_USERNAME)
GIT_TOKEN ?= $(GITLABPACKAGE_PASSWORD)
#TODO get it from git tags
BASE_VER= 0.0.1
VER = $(BASE_VER)-$(GIT_SHA)
CLIENT_VERSION = $(shell cat ./version | grep  "client-version=*" | awk -F"=" '{ print $$2 }')

docker-static-checks:
	docker run --rm -v $(shell pwd):$(shell pwd) -w $(shell pwd) python make static-checks

docker-flake8:
	docker run --rm -v $(shell pwd):$(shell pwd) -w $(shell pwd) python make flake8

docker-mypy:
	docker run --rm -v $(shell pwd):$(shell pwd) -w $(shell pwd) python make mypy

static-checks: format flake8 mypy

flake8:
	git fetch origin main
	pip3 install black flake8 wemake-python-styleguide darker==1.6.1 flake8-print
	darker --check --skip-string-normalization --revision origin/main -l 120 -L flake8 app tests

mypy:
	git fetch origin main
	pip3 install black mypy darker==1.6.1
	darker --diff --check --skip-string-normalization --revision origin/main -l 120 -L mypy app

format:
	darker --skip-string-normalization --revision origin/main -i -l 120 app tests

docker-login:
	docker login -u $(REGISTRY_USER) -p $(REGISTRY_PASSWORD) $(REGISTRY)

delete-images:
	@images_to_delete=$$(docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}' | grep -v -E "latest|x86_64-436955cb" | awk 'NF>1{print $$2}'); \
	if [ -n "$$images_to_delete" ]; then \
	    echo $$images_to_delete | xargs -r docker rmi -f 2>/dev/null || true; \
	else \
	    echo "No images to delete"; \
	fi;

delete-containers:
	@containers=$$(docker ps -aq -f "status=exited"); \
	if [ -n "$$containers" ]; then \
	    echo $$containers | xargs docker rm; \
	else \
	    echo "No stopped containers to remove"; \
	fi

build-container-image: docker-login
	# Build and push container with the git sha
	make delete-images
	docker build -t $(CONTAINER_IMAGE_NAME):$(GIT_SHA) -f backend.dockerfile --build-arg POETRY_HTTP_BASIC_GITLABPACKAGE_USERNAME=$(GIT_USERNAME) --build-arg POETRY_HTTP_BASIC_GITLABPACKAGE_PASSWORD=$(GIT_TOKEN) --network host .
	docker push $(CONTAINER_IMAGE_NAME):$(GIT_SHA)
	make delete-containers

push-custom-tag: docker-login
	docker pull $(CONTAINER_IMAGE_NAME):$(GIT_SHA)
	docker tag $(CONTAINER_IMAGE_NAME):$(GIT_SHA) $(CONTAINER_IMAGE_NAME):$(CONTAINER_TAG)
	docker push $(CONTAINER_IMAGE_NAME):$(CONTAINER_TAG)

test-without-teardown:
	 pytest --cov=app tests/ --keepalive -v -s

test:
	 pytest --cov=app tests/ -v -s

pre-test:
	poetry install --no-root
	pip-audit --ignore-vuln GHSA-w596-4wvx-j9j6 --ignore-vuln GHSA-v5gw-mw7f-84px --ignore-vuln GHSA-8fww-64cx-x8p5 --ignore-vuln PYSEC-2023-46 --ignore-vuln GHSA-5cpq-8wj7-hf2v --ignore-vuln PYSEC-2023-73 --ignore-vuln GHSA-5w5m-pfw9-c8fp --ignore-vuln PYSEC-2023-112 --ignore-vuln PYSEC-2020-96 --ignore-vuln PYSEC-2021-142 --ignore-vuln GHSA-jm77-qphf-c4w8 --ignore-vuln GHSA-v8gr-m533-ghj9 --ignore-vuln PYSEC-2023-175 --ignore-vuln GHSA-j7hp-h8jx-5ppr --ignore-vuln GHSA-56pw-mpj4-fxww --ignore-vuln GHSA-v845-jxx5-vc9f --ignore-vuln GHSA-g4mx-q9vg-27p4 --ignore-vuln PYSEC-2023-227 --ignore-vuln GHSA-jfhm-5ghh-2f97 --ignore-vuln GHSA-45x7-px36-x8w8 --ignore-vuln GHSA-h5c8-rqwp-cp95 --ignore-vuln GHSA-j225-cvw7-qrx7 --ignore-vuln GHSA-3f63-hfp8-52jq --ignore-vuln GHSA-5h86-8mv2-jq9f --ignore-vuln GHSA-8qpw-xqxj-h4r2 --ignore-vuln GHSA-3ww4-gg4f-jr7f --ignore-vuln GHSA-9v9h-cgj8-h64p --ignore-vuln GHSA-6vqw-3v5j-54x4 --ignore-vuln PYSEC-2024-48  --ignore-vuln GHSA-44wm-f244-xhp3 --ignore-vuln GHSA-cr6f-gf5w-vhrc --ignore-vuln GHSA-jjg7-2v4v-x38h --ignore-vuln GHSA-7gpw-8wmc-pm8g

format-fix:
	pip3 install isort autoflake autopep8 pyformat black
	# Sort imports one per line, so autoflake can remove unused imports
	isort --force-single-line-imports  app tests
	# Order is important please don't change it, unless you know what you are doing
	black app tests
	autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place app tests --exclude=__init__.py
	autopep8 -r --in-place -a -a --max-line-length 120 app tests
	pyformat -r -i -a  app tests
	isort app tests

upload-to-s3-dev:
	echo "$(GIT_SHA)" | aws s3 cp - s3://wmxqvbdaajqfsodgagoisxgngzamqpex/latest-tag.txt --acl public-read
upload_to_s3_poc:
	echo "$(GIT_SHA)" | aws s3 cp - s3://ezl7u4ytyo-s5cwguevl7vglyy8y-hd1s8/latest-tag.txt --acl public-read
