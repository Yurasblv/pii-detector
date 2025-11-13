#! /usr/bin/env bash
export PYTHONPATH=$PWD
export SCANNING_MODE=CUSTOMER_ACCOUNT
python app/customer_worker.py
