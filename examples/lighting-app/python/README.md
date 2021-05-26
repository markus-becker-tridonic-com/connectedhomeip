# README

## TODO

[-] tie in lighting-common via BUILD.gn
[x] enabled on/off & level control server cluster in src/controller/data_model
[x] install callbacks
[x] run chip event loop in thread
[ ] tie in python-dali
[ ] compile c parts for RPi

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
