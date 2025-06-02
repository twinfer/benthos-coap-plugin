{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.go
    pkgs.kaitai-struct-compiler
    pkgs.gotools
    pkgs.python3
    pkgs.python3Packages.pip
    pkgs.ripgrep
  ];
}