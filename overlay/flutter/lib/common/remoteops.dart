import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:flutter_hbb/common.dart';
import 'package:flutter_hbb/models/platform_model.dart';
import 'package:flutter_hbb/utils/http_service.dart' as http;

const kRemoteOpsBuiltinForceServer = 'force-builtin-server';
const kRemoteOpsAccessToken = 'access_token';
const kRemoteOpsUserInfo = 'user_info';
const kRemoteOpsSelectedNode = 'remoteops_selected_node';
const kRemoteOpsRuntimeAssignment = 'remoteops_runtime_assignment';
const kRemoteOpsBootstrapCache = 'remoteops_bootstrap_cache';
const kRemoteOpsUsername = 'remoteops_username';
const kRemoteOpsPackageCode = 'remoteops_package_code';
const kRemoteOpsProfileName = 'remoteops_profile_name';
const kRemoteOpsFixedPassword = 'remoteops_fixed_password';
const kRemoteOpsDeviceEnabled = 'remoteops_device_enabled';
const kRemoteOpsExpiresAt = 'remoteops_expires_at';

const kRemoteOpsFixedIdServer = 'rdx-runtime-fixed-id-server';
const kRemoteOpsFixedApiServer = 'rdx-runtime-fixed-api-server';
const kRemoteOpsSelectedRelayServer = 'rdx-runtime-selected-node-relay-server';
const kRemoteOpsFixedPublicKey = 'rdx-runtime-fixed-public-key';

const kRemoteOpsAutoSelectionValue = '__AUTO__';

bool isRemoteOpsMode() =>
    bind.mainGetBuildinOption(key: kRemoteOpsBuiltinForceServer) == 'Y';

String _asString(dynamic value) {
  if (value == null) {
    return '';
  }
  return value.toString().trim();
}

bool _asBool(dynamic value, {bool defaultValue = true}) {
  if (value is bool) {
    return value;
  }
  final text = _asString(value).toLowerCase();
  if (text.isEmpty) {
    return defaultValue;
  }
  if (text == 'y' || text == 'yes' || text == 'true' || text == '1') {
    return true;
  }
  if (text == 'n' || text == 'no' || text == 'false' || text == '0') {
    return false;
  }
  return defaultValue;
}

double? _asDouble(dynamic value) {
  if (value == null) {
    return null;
  }
  if (value is num) {
    return value.toDouble();
  }
  return double.tryParse(value.toString());
}

int? _asInt(dynamic value) {
  if (value == null) {
    return null;
  }
  if (value is num) {
    return value.toInt();
  }
  return int.tryParse(value.toString());
}

Map<String, dynamic> _asMap(dynamic value) {
  if (value is Map<String, dynamic>) {
    return value;
  }
  if (value is Map) {
    return Map<String, dynamic>.from(value);
  }
  return <String, dynamic>{};
}

Map<String, dynamic> _decodeStoredMap(String raw) {
  if (raw.trim().isEmpty) {
    return <String, dynamic>{};
  }
  try {
    return _asMap(jsonDecode(raw));
  } catch (e) {
    debugPrint('Failed to decode remoteops payload: $e');
    return <String, dynamic>{};
  }
}

Map<String, dynamic> _unwrapResponse(dynamic value) {
  final payload = _asMap(value);
  if (payload['data'] is Map || payload['data'] is Map<String, dynamic>) {
    return _asMap(payload['data']);
  }
  return payload;
}

String _storedUsername() {
  final explicit = bind.mainGetLocalOption(key: kRemoteOpsUsername).trim();
  if (explicit.isNotEmpty) {
    return explicit;
  }
  final userInfo =
      _decodeStoredMap(bind.mainGetLocalOption(key: kRemoteOpsUserInfo));
  return _asString(userInfo['name']);
}

class RemoteOpsServiceNode {
  RemoteOpsServiceNode({
    required this.code,
    required this.name,
    required this.region,
    required this.relayServer,
    required this.probeHost,
    required this.manualSelectable,
    required this.weight,
    required this.healthScore,
    required this.latencyMs,
    required this.probeLatencyMs,
  });

  final String code;
  final String name;
  final String region;
  final String relayServer;
  final String probeHost;
  final bool manualSelectable;
  final int weight;
  final double? healthScore;
  final int? latencyMs;
  final int? probeLatencyMs;

  factory RemoteOpsServiceNode.fromJson(Map<String, dynamic> json) {
    return RemoteOpsServiceNode(
      code: _asString(json['code']),
      name: _asString(json['name']),
      region: _asString(json['region']),
      relayServer: _asString(json['relayServer']),
      probeHost: _asString(json['probeHost']),
      manualSelectable: _asBool(json['manualSelectable']),
      weight: _asInt(json['weight']) ?? 0,
      healthScore: _asDouble(json['healthScore']),
      latencyMs: _asInt(json['latencyMs'] ?? json['latency']),
      probeLatencyMs: _asInt(json['probeLatencyMs']),
    );
  }

  String get displayName {
    if (name.isNotEmpty) {
      return name;
    }
    if (code.isNotEmpty) {
      return code;
    }
    return relayServer;
  }

  String get detailLine {
    final parts = <String>[
      if (region.isNotEmpty) region,
      if (relayServer.isNotEmpty) relayServer,
      if (probeHost.isNotEmpty) 'probe: $probeHost',
      if (latencyMs != null && latencyMs! > 0) '${latencyMs}ms',
      if (probeLatencyMs != null && probeLatencyMs! > 0) 'probe ${probeLatencyMs}ms',
      if (!manualSelectable) 'auto only',
    ];
    return parts.join(' · ');
  }

  double get priorityScore {
    if (healthScore != null) {
      return (healthScore! * 100000.0) + weight;
    }
    final latency = probeLatencyMs ?? latencyMs;
    if (latency != null && latency > 0) {
      return 100000.0 - latency + weight;
    }
    return weight.toDouble();
  }
}

class RemoteOpsProfile {
  RemoteOpsProfile({
    required this.raw,
    required this.packageCode,
    required this.profileName,
    required this.fixedPassword,
    required this.deviceEnabled,
    required this.expiresAt,
    required this.fixedIdServer,
    required this.fixedApiServer,
    required this.fixedPublicKey,
    required this.autoSelectMode,
    required this.allowManualSelection,
    required this.deviceOwner,
    required this.serviceNodes,
  });

  final Map<String, dynamic> raw;
  final String packageCode;
  final String profileName;
  final String fixedPassword;
  final bool deviceEnabled;
  final String expiresAt;
  final String fixedIdServer;
  final String fixedApiServer;
  final String fixedPublicKey;
  final String autoSelectMode;
  final bool allowManualSelection;
  final String deviceOwner;
  final List<RemoteOpsServiceNode> serviceNodes;

  factory RemoteOpsProfile.fromJson(Map<String, dynamic> json) {
    final rawNodes = json['serviceNodes'] ?? json['nodes'] ?? [];
    final nodes = <RemoteOpsServiceNode>[];
    if (rawNodes is List) {
      for (final item in rawNodes) {
        final node = RemoteOpsServiceNode.fromJson(_asMap(item));
        if (node.code.isNotEmpty || node.relayServer.isNotEmpty) {
          nodes.add(node);
        }
      }
    }
    return RemoteOpsProfile(
      raw: _asMap(json),
      packageCode: _asString(json['packageCode']),
      profileName: _asString(json['profileName']),
      fixedPassword: _asString(json['fixedPassword']),
      deviceEnabled: _asBool(json['deviceEnabled']),
      expiresAt: _asString(json['expiresAt']),
      fixedIdServer: _asString(json['fixedIdServer']),
      fixedApiServer: _asString(json['fixedApiServer']),
      fixedPublicKey: _asString(json['fixedPublicKey']),
      autoSelectMode: _asString(json['autoSelectMode']),
      allowManualSelection: _asBool(json['allowManualSelection']),
      deviceOwner: _asString(json['deviceOwner']),
      serviceNodes: nodes,
    );
  }

  bool get hasRoutingConfig =>
      fixedIdServer.isNotEmpty ||
      fixedApiServer.isNotEmpty ||
      fixedPublicKey.isNotEmpty ||
      serviceNodes.isNotEmpty;

  RemoteOpsServiceNode? findNode(String code) {
    for (final node in serviceNodes) {
      if (node.code == code) {
        return node;
      }
    }
    return null;
  }
}

RemoteOpsProfile? getRemoteOpsRuntimeProfile() {
  final payload =
      _decodeStoredMap(bind.mainGetLocalOption(key: kRemoteOpsRuntimeAssignment));
  if (payload.isEmpty) {
    return null;
  }
  return RemoteOpsProfile.fromJson(payload);
}

RemoteOpsProfile? getRemoteOpsBootstrapProfile() {
  final payload =
      _decodeStoredMap(bind.mainGetLocalOption(key: kRemoteOpsBootstrapCache));
  if (payload.isEmpty) {
    return null;
  }
  return RemoteOpsProfile.fromJson(payload);
}

RemoteOpsProfile? getRemoteOpsPreferredProfile() {
  final runtime = getRemoteOpsRuntimeProfile();
  if (runtime != null && runtime.hasRoutingConfig) {
    return runtime;
  }
  final bootstrap = getRemoteOpsBootstrapProfile();
  if (bootstrap != null && bootstrap.hasRoutingConfig) {
    return bootstrap;
  }
  return null;
}

RemoteOpsServiceNode? choosePreferredRemoteOpsNode(
    List<RemoteOpsServiceNode> nodes) {
  if (nodes.isEmpty) {
    return null;
  }
  final items = List<RemoteOpsServiceNode>.from(nodes);
  items.sort((a, b) => b.priorityScore.compareTo(a.priorityScore));
  return items.first;
}

RemoteOpsServiceNode? resolveRemoteOpsNode(
  RemoteOpsProfile profile, {
  String? preferredCode,
}) {
  final preferred = preferredCode?.trim() ?? '';
  final manualCode = preferred.isNotEmpty
      ? preferred
      : bind.mainGetLocalOption(key: kRemoteOpsSelectedNode).trim();
  if (manualCode.isNotEmpty && profile.allowManualSelection) {
    final selected = profile.findNode(manualCode);
    if (selected != null && selected.manualSelectable) {
      return selected;
    }
  }
  return choosePreferredRemoteOpsNode(profile.serviceNodes);
}

String remoteOpsRoutingSummary() {
  final profile = getRemoteOpsPreferredProfile();
  if (profile == null || !profile.hasRoutingConfig) {
    return 'Waiting for node profile';
  }
  final manualCode = bind.mainGetLocalOption(key: kRemoteOpsSelectedNode).trim();
  final selected = resolveRemoteOpsNode(profile, preferredCode: manualCode);
  final mode = manualCode.isEmpty ? 'Auto' : 'Manual';
  final target = selected?.displayName ?? 'Unavailable';
  return '$mode · $target · ${profile.serviceNodes.length} nodes';
}

Future<void> _setLocalOption(String key, String value) async {
  await bind.mainSetLocalOption(key: key, value: value);
}

Future<void> cacheRemoteOpsRuntimeProfile(
  RemoteOpsProfile profile, {
  String? username,
}) async {
  await Future.wait([
    _setLocalOption(kRemoteOpsRuntimeAssignment, jsonEncode(profile.raw)),
    _setLocalOption(kRemoteOpsUsername, (username ?? _storedUsername()).trim()),
    _setLocalOption(kRemoteOpsPackageCode, profile.packageCode),
    _setLocalOption(kRemoteOpsProfileName, profile.profileName),
    _setLocalOption(kRemoteOpsFixedPassword, profile.fixedPassword),
    _setLocalOption(kRemoteOpsDeviceEnabled, profile.deviceEnabled ? 'Y' : 'N'),
    _setLocalOption(kRemoteOpsExpiresAt, profile.expiresAt),
  ]);
}

Future<void> cacheRemoteOpsBootstrapProfile(RemoteOpsProfile profile) async {
  await Future.wait([
    _setLocalOption(kRemoteOpsBootstrapCache, jsonEncode(profile.raw)),
    _setLocalOption(kRemoteOpsPackageCode, profile.packageCode),
  ]);
}

Future<RemoteOpsServiceNode?> applyRemoteOpsNodeSelection(
  RemoteOpsProfile profile, {
  String? preferredCode,
}) async {
  final preferred = preferredCode?.trim() ?? '';
  final rawPreferred = preferred.isNotEmpty
      ? preferred
      : bind.mainGetLocalOption(key: kRemoteOpsSelectedNode).trim();
  String manualCode = '';
  if (rawPreferred.isNotEmpty && profile.allowManualSelection) {
    final selected = profile.findNode(rawPreferred);
    if (selected != null && selected.manualSelectable) {
      manualCode = rawPreferred;
    }
  }
  final selected = resolveRemoteOpsNode(profile, preferredCode: manualCode);
  await Future.wait([
    _setLocalOption(kRemoteOpsSelectedNode, manualCode),
    _setLocalOption(kRemoteOpsFixedIdServer, profile.fixedIdServer),
    _setLocalOption(kRemoteOpsFixedApiServer, profile.fixedApiServer),
    _setLocalOption(kRemoteOpsFixedPublicKey, profile.fixedPublicKey),
    _setLocalOption(
        kRemoteOpsSelectedRelayServer, selected?.relayServer ?? ''),
    bind.mainSetOption(key: 'relay-server', value: selected?.relayServer ?? ''),
  ]);
  return selected;
}

Future<RemoteOpsProfile?> fetchRemoteOpsRuntimeAssignment({
  String? username,
}) async {
  if (!isRemoteOpsMode()) {
    return null;
  }
  final token = bind.mainGetLocalOption(key: kRemoteOpsAccessToken).trim();
  final apiServer = (await bind.mainGetApiServer()).trim();
  if (token.isEmpty || apiServer.isEmpty) {
    return null;
  }
  final id = await bind.mainGetMyId();
  final uuid = await bind.mainGetUuid();
  try {
    final resp = await http.get(
      Uri.parse('$apiServer/api/client/runtime-assignment'),
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer $token',
        'x-rustdesk-id': id,
        'x-rustdesk-uuid': uuid,
      },
    );
    if (resp.statusCode < 200 || resp.statusCode >= 300) {
      debugPrint(
          'remoteops runtime assignment failed: HTTP ${resp.statusCode}');
      return null;
    }
    final payload = _unwrapResponse(jsonDecode(decode_http_response(resp)));
    if (payload.isEmpty) {
      return null;
    }
    final profile = RemoteOpsProfile.fromJson(payload);
    await cacheRemoteOpsRuntimeProfile(
      profile,
      username: (username ?? _storedUsername()).trim(),
    );
    await applyRemoteOpsNodeSelection(profile);
    return profile;
  } catch (e) {
    debugPrint('remoteops runtime assignment failed: $e');
    return null;
  }
}

String getRemoteOpsBootstrapUrl(String packageCode, String apiServer) {
  final normalizedApi = apiServer.trim();
  final normalizedCode = packageCode.trim();
  if (normalizedApi.isEmpty || normalizedCode.isEmpty) {
    return '';
  }
  return '$normalizedApi/api/runtime-packages/bootstrap/$normalizedCode';
}

Future<RemoteOpsProfile?> fetchRemoteOpsBootstrap() async {
  if (!isRemoteOpsMode()) {
    return null;
  }
  final packageCode = bind.mainGetLocalOption(key: kRemoteOpsPackageCode).trim();
  if (packageCode.isEmpty) {
    return null;
  }
  final apiServer = (await bind.mainGetApiServer()).trim();
  final url = getRemoteOpsBootstrapUrl(packageCode, apiServer);
  if (url.isEmpty) {
    return null;
  }
  try {
    final resp = await http.get(Uri.parse(url), headers: {
      'Content-Type': 'application/json',
    });
    if (resp.statusCode < 200 || resp.statusCode >= 300) {
      debugPrint('remoteops bootstrap failed: HTTP ${resp.statusCode}');
      return null;
    }
    final payload = _unwrapResponse(jsonDecode(decode_http_response(resp)));
    if (payload.isEmpty) {
      return null;
    }
    final profile = RemoteOpsProfile.fromJson(payload);
    await cacheRemoteOpsBootstrapProfile(profile);
    await applyRemoteOpsNodeSelection(profile);
    return profile;
  } catch (e) {
    debugPrint('remoteops bootstrap failed: $e');
    return null;
  }
}

Future<RemoteOpsProfile?> loadRemoteOpsRouting({
  bool refreshRuntime = true,
  bool fetchBootstrapIfMissing = true,
}) async {
  if (!isRemoteOpsMode()) {
    return null;
  }
  if (refreshRuntime &&
      bind.mainGetLocalOption(key: kRemoteOpsAccessToken).trim().isNotEmpty) {
    final runtime = await fetchRemoteOpsRuntimeAssignment();
    if (runtime != null && runtime.hasRoutingConfig) {
      return runtime;
    }
  }
  final cachedRuntime = getRemoteOpsRuntimeProfile();
  if (cachedRuntime != null && cachedRuntime.hasRoutingConfig) {
    return cachedRuntime;
  }
  final cachedBootstrap = getRemoteOpsBootstrapProfile();
  if (cachedBootstrap != null && cachedBootstrap.hasRoutingConfig) {
    return cachedBootstrap;
  }
  if (fetchBootstrapIfMissing) {
    return await fetchRemoteOpsBootstrap();
  }
  return null;
}

Future<void> primeRemoteOpsRouting() async {
  if (!isRemoteOpsMode()) {
    return;
  }
  final profile = await loadRemoteOpsRouting(
    refreshRuntime:
        bind.mainGetLocalOption(key: kRemoteOpsAccessToken).trim().isNotEmpty,
    fetchBootstrapIfMissing: true,
  );
  if (profile != null && profile.hasRoutingConfig) {
    await applyRemoteOpsNodeSelection(profile);
  }
}

Future<void> resetRemoteOpsRuntimeState() async {
  await Future.wait([
    _setLocalOption(kRemoteOpsRuntimeAssignment, ''),
    _setLocalOption(kRemoteOpsUsername, ''),
    _setLocalOption(kRemoteOpsFixedPassword, ''),
    _setLocalOption(kRemoteOpsDeviceEnabled, ''),
    _setLocalOption(kRemoteOpsExpiresAt, ''),
  ]);
  final bootstrap = getRemoteOpsBootstrapProfile();
  if (bootstrap != null && bootstrap.hasRoutingConfig) {
    await applyRemoteOpsNodeSelection(bootstrap);
  } else {
    await Future.wait([
      _setLocalOption(kRemoteOpsFixedIdServer, ''),
      _setLocalOption(kRemoteOpsFixedApiServer, ''),
      _setLocalOption(kRemoteOpsFixedPublicKey, ''),
      _setLocalOption(kRemoteOpsSelectedRelayServer, ''),
      bind.mainSetOption(key: 'relay-server', value: ''),
    ]);
  }
}
