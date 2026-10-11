# Contributing to redissub

Thank you for your interest in contributing to `redissub`!

## Guidelines

1. **Standalone Crate**: `redissub` is an independent crate with no workspace dependencies.
2. **Quality & Tests**: Ensure all code compiles cleanly without warnings and that existing and new tests pass:
   ```bash
   cargo test
   ```
3. **Pure Rust Wire Protocol**: Avoid external C dependencies; protocol codecs should remain pure Rust.
4. **License & Attribution**: Contributions are licensed under the MIT License. Please do not add automated or AI attribution in code comments or commit logs.
