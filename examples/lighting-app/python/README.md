# README

## TODO

* [x] enabled on/off & level control server cluster in src/controller/data_model
* [x] install callbacks
* [x] run chip event loop in thread
* [x] tie in python-dali
* [ ] compile c parts for RPi
* [ ] use dedicated zap file
* [ ] export cluster and attribute IDs to python API
* [ ] enable CHIP rendezvous
* [ ] enumerate devices on DALI line and create endpoints dynamically

## EXAMPLE

```shell
pip3 install python-dali

cd examples/lighting-app/python
python lighting.py
```

```shell
source scripts/activate.sh

cd examples/chip-tool

gn gen out/debug --args='chip_bypass_rendezvous=true'
ninja -C out/debug

./out/debug/chip-tool pairing bypass 127.0.0.1 11097
./out/debug/chip-tool onoff on 1
./out/debug/chip-tool onoff off 1
```

## NOTES

```shell
avahi-browse -v -r _chipc._udp
```
