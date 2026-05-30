# linux-x64 vendored binaries

Empty placeholder. The Linux Arrow binary can't be produced from a Windows
checkout — generate it on a Linux machine or in CI:

```bash
./scripts/vendor-conan-arrow.sh   # writes arrow.tgz here
```

Then commit `arrow.tgz` (tracked via Git LFS). See `../README.md`.
