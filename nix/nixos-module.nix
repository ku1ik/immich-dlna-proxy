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
    log_level = cfg.logLevel;
  };
  addressMatch = builtins.match "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+:([0-9]{1,5})" cfg.listenAddress;
  # Use an invalid port on a format mismatch so the assertion explains the failure.
  port = if addressMatch == null then 0 else lib.toIntBase10 (builtins.head addressMatch);
  validPort = port >= 1024 && port <= 65535;
  # Normalize repeated separators and .. before checking for a Nix store path.
  normalizedSecretPath = toString (/. + cfg.immichApiKeyFile);
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
      description = "Open the HTTP TCP port and SSDP UDP port 1900 on all interfaces. DLNA is unauthenticated; use only on a trusted network.";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion =
          lib.hasPrefix "/" cfg.immichApiKeyFile
          && !builtins.hasContext cfg.immichApiKeyFile
          && normalizedSecretPath != builtins.storeDir
          && !lib.hasPrefix "${builtins.storeDir}/" normalizedSecretPath;
        message = "services.immich-dlna-proxy.immichApiKeyFile must be an absolute runtime path string outside the Nix store, not secret contents or a store reference.";
      }
      {
        assertion = !cfg.openFirewall || validPort;
        message = "services.immich-dlna-proxy.listenAddress must use IPv4:PORT with a decimal port in 1024-65535 to open the firewall.";
      }
      {
        # LoadCredential is written directly into the unit: reject line breaks
        # and a trailing backslash, which systemd treats as a line continuation.
        assertion =
          !lib.hasInfix "\n" cfg.immichApiKeyFile
          && !lib.hasInfix "\r" cfg.immichApiKeyFile
          && !lib.hasSuffix "\\" cfg.immichApiKeyFile;
        message = "services.immich-dlna-proxy.immichApiKeyFile cannot contain line breaks or end with a backslash.";
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
        # LoadCredential does not accept a quoted ID:path pair.
        LoadCredential = [ "immich-api-key:${cfg.immichApiKeyFile}" ];
        DynamicUser = true;
        UMask = "0077";
        Restart = "on-failure";
        RestartSec = 3;

        # Keep the hardening policy explicit, including DynamicUser's implied settings.
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

    networking.firewall = lib.mkIf cfg.openFirewall {
      allowedTCPPorts = lib.optional validPort port;
      allowedUDPPorts = [ 1900 ];
    };
  };
}
