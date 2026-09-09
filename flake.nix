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
              ./tests
            ];
          };
          cargoLock.lockFile = ./Cargo.lock;
          postInstall = ''
            install -Dm644 LICENSE $out/share/licenses/immich-dlna-proxy/LICENSE
            install -Dm644 README.md $out/share/doc/immich-dlna-proxy/README.md
          '';
          meta = {
            description = "Immich DLNA proxy";
            homepage = "https://github.com/ku1ik/immich-dlna-proxy";
            license = pkgs.lib.licenses.asl20;
            mainProgram = "immich-dlna-proxy";
            platforms = pkgs.lib.platforms.linux;
          };
        };
        default = immich-dlna-proxy;
      }) nixpkgs.legacyPackages;

      nixosModules.default = nixpkgs.lib.modules.importApply ./nix/nixos-module.nix {
        defaultPackage = pkgs: self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      };

      checks = nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ] (system: {
        module-eval = import ./nix/module-tests.nix {
          inherit nixpkgs;
          pkgs = nixpkgs.legacyPackages.${system};
          module = self.nixosModules.default;
          defaultPackage = self.packages.${system}.default;
        };
      });

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
            ffmpeg
          ];

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
        };
      }) nixpkgs.legacyPackages;
    };
}
