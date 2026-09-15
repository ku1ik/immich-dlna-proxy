{ defaultPackage }:
{
  config,
  lib,
  pkgs,
  utils,
  ...
}:
let
  cfg = config.services.immich-dlna-proxy;
  format = pkgs.formats.toml { };
  configFile = format.generate "immich-dlna-proxy.toml" {
    immich_url = cfg.immichUrl;
    immich_api_key_file = "/run/credentials/immich-dlna-proxy.service/immich-api-key";
    listen_address = cfg.listenAddress;
    friendly_name = cfg.friendlyName;
    sort_locale = cfg.sortLocale;
    server_uuid = cfg.serverUuid;
    state_directory = "/var/lib/immich-dlna-proxy";
    log_level = cfg.logLevel;
  };
  addressMatch = builtins.match "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+:([0-9]{1,5})" cfg.listenAddress;
  port = if addressMatch == null then 0 else lib.toIntBase10 (builtins.head addressMatch);
  validInterface =
    cfg.firewallInterface != null
    && cfg.firewallInterface != "default"
    && builtins.match "[a-zA-Z0-9_.-]+" cfg.firewallInterface != null;
  secretPath = toString (/. + cfg.immichApiKeyFile);
in
{
  options.services.immich-dlna-proxy = {
    enable = lib.mkEnableOption "the Immich DLNA proxy";

    package = lib.mkOption {
      type = lib.types.package;
      default = defaultPackage pkgs;
      defaultText = lib.literalExpression "immich-dlna-proxy.packages.${pkgs.stdenv.hostPlatform.system}.default";
      description = "Package providing the immich-dlna-proxy executable.";
    };

    immichUrl = lib.mkOption {
      type = lib.types.str;
      example = "https://immich.example.com";
      description = "Absolute HTTP(S) Immich URL without userinfo, query or fragment.";
    };

    immichApiKeyFile = lib.mkOption {
      type = lib.types.str;
      example = "/run/secrets/immich-dlna-api-key";
      description = ''
        Absolute runtime path to the API key, outside the Nix store. Use a string,
        not a Nix path literal or file contents. Systemd reads it with LoadCredential;
        provision and rotate it externally, then restart the service.
      '';
    };

    listenAddress = lib.mkOption {
      type = lib.types.str;
      example = "192.168.1.10:8200";
      description = "Concrete local unicast IPv4 address and port (1024-65535), also used for SSDP interface selection.";
    };

    friendlyName = lib.mkOption {
      type = lib.types.str;
      default = "Immich";
      description = "DLNA server name: 1-128 UTF-8 bytes of XML-safe text.";
    };

    sortLocale = lib.mkOption {
      type = lib.types.nonEmptyStr;
      example = "pl";
      description = "Explicit ICU4X locale for case-insensitive album-title ties in latest-asset-date ordering; never inferred from the host.";
    };

    serverUuid = lib.mkOption {
      type = lib.types.str;
      example = "7b37df49-b75d-4bcb-89a6-0c917a934643";
      description = "Stable, non-nil server UUID. Generate once and retain across restarts.";
    };

    logLevel = lib.mkOption {
      type = lib.types.enum [
        "error"
        "warn"
        "info"
        "debug"
        "trace"
      ];
      default = "info";
      description = "Minimum log level sent to the journal.";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open the HTTP TCP port and SSDP UDP port 1900 on firewallInterface only. DLNA is unauthenticated; use a trusted LAN.";
    };

    firewallInterface = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "enp3s0";
      description = ''
        Required when openFirewall is enabled. Exact trusted LAN interface name
        using letters, digits, underscores, dots or hyphens; no wildcards or the
        reserved NixOS name "default". Must correspond to listenAddress.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion =
          lib.hasPrefix "/" cfg.immichApiKeyFile
          && !builtins.hasContext cfg.immichApiKeyFile
          && secretPath != builtins.storeDir
          && !lib.hasPrefix "${builtins.storeDir}/" secretPath;
        message = "services.immich-dlna-proxy.immichApiKeyFile must be an absolute runtime path string outside the Nix store, not secret contents or a store reference.";
      }
      {
        assertion = !cfg.openFirewall || validInterface;
        message = "services.immich-dlna-proxy.firewallInterface must name one nonempty LAN interface when openFirewall is true (letters, digits, _, ., -; not 'default' or a wildcard).";
      }
      {
        assertion = !cfg.openFirewall || (port >= 1024 && port <= 65535);
        message = "services.immich-dlna-proxy.listenAddress must use IPv4:PORT with a decimal port in 1024-65535 to open the firewall.";
      }
    ];

    systemd.services.immich-dlna-proxy = {
      description = "Immich DLNA proxy";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      serviceConfig = {
        ExecStart = utils.escapeSystemdExecArgs [
          (lib.getExe cfg.package)
          "--config"
          configFile
        ];
        # LoadCredential expands % specifiers, but unlike ExecStart not $ variables.
        LoadCredential = [
          (builtins.toJSON (lib.replaceStrings [ "%" ] [ "%%" ] "immich-api-key:${cfg.immichApiKeyFile}"))
        ];
        DynamicUser = true;
        StateDirectory = "immich-dlna-proxy";
        StateDirectoryMode = "0700";
        UMask = "0077";
        Restart = "on-failure";
        RestartSec = 3;
        TimeoutStopSec = 15;

        NoNewPrivileges = true;
        CapabilityBoundingSet = "";
        AmbientCapabilities = "";
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = true;
        RestrictSUIDSGID = true;
        # Keep host networking available for multicast and interface discovery.
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
          "AF_NETLINK"
        ];
      };
    };

    networking.firewall.interfaces = lib.mkIf (cfg.openFirewall && validInterface) {
      ${cfg.firewallInterface} = {
        allowedTCPPorts = lib.optional (port >= 1024 && port <= 65535) port;
        allowedUDPPorts = [ 1900 ];
      };
    };
  };
}
