# mskrc

Generate configuration files for some Kafka CLI tools from AWS MSK clusters.

## Install

```bash
go install code.selman.me/mskrc/cmd/mskrc@latest
```

## Usage

```bash
# kaf
mskrc kaf

# kcl
mskrc kcl

# kafkactl
mskrc kafkactl

# Filter specific clusters and set aliases
mskrc kaf --cluster my-cluster --alias my-cluster:prod
```
