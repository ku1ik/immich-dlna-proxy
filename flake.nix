{
  description = "Immich DLNA proxy";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
  };

  outputs =
    { self, nixpkgs }:
    {
      packages = builtins.mapAttrs (_system: pkgs: rec {
        immich-dlna-proxy = pkgs.rustPlatform.buildRustPackage {
          pname = "immich-dlna-proxy";
          version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).package.version;
          src = pkgs.lib.fileset.toSource {
            root = ./.;
            fileset = pkgs.lib.fileset.unions [
              ./Cargo.lock
              ./Cargo.toml
              ./LICENSE
              ./README.md
              ./src
            ];
          };
          cargoLock.lockFile = ./Cargo.lock;
          postInstall = ''
            install -Dm644 LICENSE $out/share/licenses/immich-dlna-proxy/LICENSE
          '';
          meta = {
            description = "Immich DLNA proxy";
            homepage = "https://github.com/ku1ik/immich-dlna-proxy";
            license = pkgs.lib.licenses.asl20;
            mainProgram = "immich-dlna-proxy";
            platforms = pkgs.lib.platforms.unix;
          };
        };
        default = immich-dlna-proxy;
      }) nixpkgs.legacyPackages;

      devShells = builtins.mapAttrs (_system: pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            rustc
            cargo
            clippy
            rustfmt
            rust-analyzer
            just
            bacon
          ];

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
        };
      }) nixpkgs.legacyPackages;
    };
}
