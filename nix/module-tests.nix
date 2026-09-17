{
  nixpkgs,
  pkgs,
  module,
  defaultPackage,
}:
let
  inherit (nixpkgs) lib;
  name = "immich-dlna-proxy";
  required = {
    enable = true;
    immichUrl = "https://immich.example.com/photos";
    immichApiKeyFile = "/run/secrets/immich-api-key";
    listenAddress = "192.168.1.10:8200";
    sortLocale = "pl";
    serverUuid = "7b37df49-b75d-4bcb-89a6-0c917a934643";
  };
  evaluateWith =
    testPkgs: settings:
    nixpkgs.lib.nixosSystem {
      modules = [
        module
        {
          nixpkgs.pkgs = testPkgs;
          boot.isContainer = true;
          system.stateVersion = "26.05";
          services.${name} = settings;
        }
      ];
    };
  evaluate = evaluateWith pkgs;
  disabled = evaluate { };
  enabled = evaluate required;
  service = enabled.config.systemd.services.${name};
  firewall = evaluate (required // { openFirewall = true; });
  valid =
    evaluated:
    let
      failures = lib.filter (a: !a.assertion) evaluated.config.assertions;
    in
    if failures != [ ] then
      throw (lib.concatMapStringsSep "\n" (a: a.message) failures)
    else
      builtins.deepSeq evaluated.config.systemd.services.${name}.serviceConfig (
        builtins.deepSeq evaluated.config.systemd.units."${name}.service".text true
      );
  rejects = settings: !(builtins.tryEval (valid (evaluate settings))).success;

  specialSecret = "/run/secrets/key %n $TOKEN \"quoted\" \\ suffix";
  escaped = evaluate (required // { immichApiKeyFile = specialSecret; });
  expectedCredential = ''"immich-api-key:/run/secrets/key %%n $TOKEN \"quoted\" \\ suffix"'';

  expectedSettings = {
    immich_url = required.immichUrl;
    immich_api_key_file = "/run/credentials/${name}.service/immich-api-key";
    listen_address = required.listenAddress;
    friendly_name = "Media \"room\" \\ TV\nSecond line";
    sort_locale = required.sortLocale;
    server_uuid = required.serverUuid;
    state_directory = "/var/lib/${name}";
    log_level = "debug";
  };
  # Inspect the TOML generator's input without realizing its derivation (no IFD).
  # A difficult output path also exercises systemd ExecStart argument escaping.
  inspected =
    evaluateWith
      (pkgs.extend (
        _final: previous: {
          formats = previous.formats // {
            toml =
              args:
              (previous.formats.toml args)
              // {
                generate =
                  fileName: settings:
                  assert fileName == "${name}.toml";
                  assert settings == expectedSettings;
                  "/config path/$name%/\"settings\".toml";
              };
          };
        }
      ))
      (
        required
        // {
          friendlyName = expectedSettings.friendly_name;
          logLevel = expectedSettings.log_level;
        }
      );
  overridden = evaluate (required // { package = pkgs.hello; });

  tests = {
    disabledByDefault =
      !disabled.config.services.${name}.enable
      && !(disabled.config.systemd.services ? ${name})
      && !(disabled.config.systemd.units ? "${name}.service")
      && disabled.config.networking.firewall.interfaces == { };
    enabled = valid enabled;
    defaults =
      enabled.config.services.${name}.package.outPath == defaultPackage.outPath
      && enabled.config.services.${name}.friendlyName == "Immich"
      && enabled.config.services.${name}.logLevel == "info"
      && !enabled.config.services.${name}.openFirewall;
    nonsecretTomlAndExecEscaping =
      valid inspected
      &&
        inspected.config.systemd.services.${name}.serviceConfig.ExecStart
        == ''"${lib.getExe defaultPackage}" "--config" "/config path/$$name%%/\"settings\".toml"'';
    credentials =
      service.serviceConfig.LoadCredential == [ ''"immich-api-key:/run/secrets/immich-api-key"'' ]
      && valid escaped
      && escaped.config.systemd.services.${name}.serviceConfig.LoadCredential == [ expectedCredential ]
      &&
        lib.hasInfix "LoadCredential=${expectedCredential}\n"
          escaped.config.systemd.units."${name}.service".text;
    privateStateAndHardening =
      service.serviceConfig.DynamicUser
      && service.serviceConfig.StateDirectory == name
      && service.serviceConfig.StateDirectoryMode == "0700"
      && service.serviceConfig.UMask == "0077"
      && service.serviceConfig.NoNewPrivileges
      && service.serviceConfig.CapabilityBoundingSet == ""
      && service.serviceConfig.AmbientCapabilities == ""
      && service.serviceConfig.ProtectSystem == "strict"
      && service.serviceConfig.ProtectHome
      && service.serviceConfig.PrivateTmp
      && service.serviceConfig.PrivateDevices
      && service.serviceConfig.RestrictSUIDSGID;
    networkAndFilesystemAccess =
      service.serviceConfig.RestrictAddressFamilies == [
        "AF_INET"
        "AF_INET6"
        "AF_UNIX"
        "AF_NETLINK"
      ]
      && !(service.serviceConfig.PrivateNetwork or false)
      && !(service.serviceConfig ? IPAddressDeny)
      && !(service.serviceConfig ? SystemCallFilter)
      && !(service.serviceConfig ? InaccessiblePaths)
      && !(service.serviceConfig ? ProcSubset);
    lifecycle =
      service.wantedBy == [ "multi-user.target" ]
      && service.wants == [ "network-online.target" ]
      && service.after == [ "network-online.target" ]
      && service.serviceConfig.Restart == "on-failure"
      && service.serviceConfig.RestartSec == 3;
    firewallClosedByDefault =
      enabled.config.networking.firewall.interfaces == { }
      &&
        enabled.config.networking.firewall.allowedTCPPorts
        == disabled.config.networking.firewall.allowedTCPPorts
      &&
        enabled.config.networking.firewall.allowedUDPPorts
        == disabled.config.networking.firewall.allowedUDPPorts;
    firewallOpen =
      valid firewall
      && firewall.config.networking.firewall.interfaces == { }
      && firewall.config.networking.firewall.allowedTCPPorts == [ 8200 ]
      && firewall.config.networking.firewall.allowedUDPPorts == [ 1900 ];
    packageOverride =
      valid overridden
      &&
        lib.hasPrefix ''"${lib.getExe pkgs.hello}" "--config" ''
          overridden.config.systemd.services.${name}.serviceConfig.ExecStart;
    logLevelEnum =
      lib.all enabled.options.services.${name}.logLevel.type.check [
        "error"
        "warn"
        "info"
        "debug"
        "trace"
      ]
      && rejects (required // { logLevel = "verbose"; });
    secretIsStringOnly = !enabled.options.services.${name}.immichApiKeyFile.type.check /run/secrets/key;
  }
  // lib.genAttrs [ "immichUrl" "immichApiKeyFile" "listenAddress" "sortLocale" "serverUuid" ] (
    field: rejects (builtins.removeAttrs required [ field ])
  )
  // lib.listToAttrs (
    map
      (address: {
        name = "rejectPort:${address}";
        value = rejects (
          required
          // {
            openFirewall = true;
            listenAddress = address;
          }
        );
      })
      [
        "192.168.1.10"
        "192.168.1.10:http"
        "192.168.1.10:0"
        "192.168.1.10:1023"
        "192.168.1.10:65536"
        "192.168.1.10:99999999999999999999"
      ]
  )
  // lib.listToAttrs (
    map
      (path: {
        name = "rejectSecret:${path}";
        value = rejects (required // { immichApiKeyFile = path; });
      })
      [
        ""
        "relative/key"
        "/nix/store/key"
        "/nix/store"
        "/run/../nix/store/key"
        "//nix/store/key"
      ]
  );
  failures = builtins.attrNames (lib.filterAttrs (_: passed: !passed) tests);
in
assert lib.assertMsg (
  failures == [ ]
) "Module evaluation checks failed: ${lib.concatStringsSep ", " failures}";
pkgs.runCommand "immich-dlna-proxy-module-eval"
  {
    passthru = { inherit tests; };
  }
  ''
    touch "$out"
  ''
