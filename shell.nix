with import <nixpkgs> {};

mkShell {
  buildInputs = [ opam ocaml autoconf automake gnum4 ncurses pkgconfig gmp zlib ];

  shellHook = ''
    eval $(opam config env)
  '';
}
