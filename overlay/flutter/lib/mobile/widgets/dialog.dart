import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_hbb/common/widgets/setting_widgets.dart';
import 'package:flutter_hbb/common/widgets/toolbar.dart';
import 'package:flutter_hbb/common/remoteops.dart';
import 'package:get/get.dart';

import '../../common.dart';
import '../../models/platform_model.dart';

void _showSuccess() {
  showToast(translate("Successful"));
}

void setTemporaryPasswordLengthDialog(
    OverlayDialogManager dialogManager) async {
  List<String> lengths = ['6', '8', '10'];
  String length = await bind.mainGetOption(key: "temporary-password-length");
  var index = lengths.indexOf(length);
  if (index < 0) index = 0;
  length = lengths[index];
  dialogManager.show((setState, close, context) {
    setLength(newValue) {
      final oldValue = length;
      if (oldValue == newValue) return;
      setState(() {
        length = newValue;
      });
      bind.mainSetOption(key: "temporary-password-length", value: newValue);
      bind.mainUpdateTemporaryPassword();
      Future.delayed(Duration(milliseconds: 200), () {
        close();
        _showSuccess();
      });
    }

    return CustomAlertDialog(
      title: Text(translate("Set one-time password length")),
      content: Row(
          mainAxisAlignment: MainAxisAlignment.spaceEvenly,
          children: lengths
              .map(
                (value) => Row(
                  children: [
                    Text(value),
                    Radio(
                        value: value, groupValue: length, onChanged: setLength),
                  ],
                ),
              )
              .toList()),
    );
  }, backDismiss: true, clickMaskDismiss: true);
}

void showServerSettings(OverlayDialogManager dialogManager,
    void Function(VoidCallback) setState) async {
  if (isRemoteOpsMode()) {
    await showRemoteOpsNodeRoutingDialog(dialogManager, setState);
    return;
  }
  Map<String, dynamic> options = {};
  try {
    options = jsonDecode(await bind.mainGetOptions());
  } catch (e) {
    print("Invalid server config: $e");
  }
  showServerSettingsWithValue(
      ServerConfig.fromOptions(options), dialogManager, setState);
}

Future<void> showRemoteOpsNodeRoutingDialog(
  OverlayDialogManager dialogManager,
  void Function(VoidCallback)? upSetState,
) async {
  final tag = gFFI.dialogManager.showLoading(translate('Waiting'));
  final profile = await loadRemoteOpsRouting(
    refreshRuntime: true,
    fetchBootstrapIfMissing: true,
  );
  gFFI.dialogManager.dismissByTag(tag);
  if (profile == null || profile.serviceNodes.isEmpty) {
    showToast('No node profile available yet');
    return;
  }
  showRemoteOpsNodeRoutingDialogWithValue(profile, dialogManager, upSetState);
}

void showRemoteOpsNodeRoutingDialogWithValue(
  RemoteOpsProfile profile,
  OverlayDialogManager dialogManager,
  void Function(VoidCallback)? upSetState,
) {
  var currentProfile = profile;
  var selectedCode = bind.mainGetLocalOption(key: kRemoteOpsSelectedNode).trim();
  if (selectedCode.isEmpty) {
    selectedCode = kRemoteOpsAutoSelectionValue;
  }
  var isInProgress = false;

  dialogManager.show((setState, close, context) {
    Widget infoRow(String label, String value) {
      if (value.trim().isEmpty) {
        return Offstage();
      }
      return Padding(
        padding: const EdgeInsets.only(bottom: 6),
        child: Row(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            SizedBox(
              width: 118,
              child: Text(
                label,
                style: TextStyle(
                  color: Theme.of(context)
                      .textTheme
                      .bodySmall
                      ?.color
                      ?.withOpacity(0.8),
                ),
              ),
            ),
            Expanded(
              child: SelectableText(
                value,
                style: TextStyle(fontSize: 13),
              ),
            ),
          ],
        ),
      );
    }

    final manualCode =
        selectedCode == kRemoteOpsAutoSelectionValue ? '' : selectedCode;
    final effectiveNode =
        resolveRemoteOpsNode(currentProfile, preferredCode: manualCode);

    Future<void> submit() async {
      setState(() {
        isInProgress = true;
      });
      await applyRemoteOpsNodeSelection(
        currentProfile,
        preferredCode:
            selectedCode == kRemoteOpsAutoSelectionValue ? '' : selectedCode,
      );
      setState(() {
        isInProgress = false;
      });
      close();
      upSetState?.call(() {});
      showToast(translate('Successful'));
    }

    Future<void> reloadProfile() async {
      setState(() {
        isInProgress = true;
      });
      final latest = await loadRemoteOpsRouting(
        refreshRuntime: true,
        fetchBootstrapIfMissing: true,
      );
      setState(() {
        isInProgress = false;
        if (latest != null) {
          currentProfile = latest;
          final current =
              bind.mainGetLocalOption(key: kRemoteOpsSelectedNode).trim();
          selectedCode = current.isEmpty ? kRemoteOpsAutoSelectionValue : current;
        }
      });
    }

    Widget buildNodeTile(RemoteOpsServiceNode node) {
      final disabled =
          !currentProfile.allowManualSelection ||
          !node.manualSelectable ||
          isInProgress;
      final isEffective = effectiveNode?.code == node.code;
      return Container(
        margin: const EdgeInsets.only(bottom: 8),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(10),
          border: Border.all(
            color: isEffective ? MyTheme.accent.withOpacity(0.45) : Colors.grey.shade300,
          ),
          color: isEffective
              ? MyTheme.accent.withOpacity(0.06)
              : Colors.transparent,
        ),
        child: RadioListTile<String>(
          value: node.code,
          groupValue: selectedCode,
          onChanged: disabled
              ? null
              : (value) {
                  if (value == null) return;
                  setState(() {
                    selectedCode = value;
                  });
                },
          title: Row(
            children: [
              Expanded(child: Text(node.displayName)),
              if (isEffective)
                Container(
                  padding:
                      const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
                  decoration: BoxDecoration(
                    borderRadius: BorderRadius.circular(999),
                    color: MyTheme.accent.withOpacity(0.12),
                  ),
                  child: Text(
                    'Current',
                    style: TextStyle(
                      color: MyTheme.accent,
                      fontSize: 11,
                    ),
                  ),
                ),
            ],
          ),
          subtitle: Padding(
            padding: const EdgeInsets.only(top: 4),
            child: Text(node.detailLine),
          ),
          dense: true,
          contentPadding:
              const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
        ),
      );
    }

    return CustomAlertDialog(
      title: Row(
        children: [
          Expanded(child: Text('Node Routing')),
          IconButton(
            icon: Icon(Icons.refresh, color: MyTheme.accent),
            onPressed: isInProgress ? null : reloadProfile,
            tooltip: translate('Retry'),
          ),
        ],
      ),
      contentBoxConstraints: const BoxConstraints(minWidth: 560, maxWidth: 680),
      content: Column(
        mainAxisSize: MainAxisSize.min,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          infoRow('Package', currentProfile.packageCode),
          infoRow('Profile', currentProfile.profileName),
          infoRow('Fixed ID Server', currentProfile.fixedIdServer),
          infoRow('Fixed API Server', currentProfile.fixedApiServer),
          infoRow('Auto Select', currentProfile.autoSelectMode),
          infoRow('Expires At', currentProfile.expiresAt),
          if (!currentProfile.allowManualSelection)
            Padding(
              padding: const EdgeInsets.only(bottom: 8),
              child: Text(
                'Manual node switching is disabled by policy. The client will keep automatic routing.',
                style: TextStyle(
                  color: Colors.orange.shade800,
                  fontSize: 12,
                ),
              ),
            ),
          Container(
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(10),
              border: Border.all(color: Colors.grey.shade300),
            ),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                RadioListTile<String>(
                  value: kRemoteOpsAutoSelectionValue,
                  groupValue: selectedCode,
                  onChanged: isInProgress
                      ? null
                      : (value) {
                          if (value == null) return;
                          setState(() {
                            selectedCode = value;
                          });
                        },
                  title: const Text('Automatic routing'),
                  subtitle: Text(
                    effectiveNode == null
                        ? 'Use the best available node automatically'
                        : 'Current auto-selected node: ${effectiveNode.displayName}',
                  ),
                  dense: true,
                  contentPadding:
                      const EdgeInsets.symmetric(horizontal: 8, vertical: 4),
                ),
                Divider(height: 1),
                ConstrainedBox(
                  constraints: const BoxConstraints(maxHeight: 320),
                  child: SingleChildScrollView(
                    child: Padding(
                      padding: const EdgeInsets.fromLTRB(12, 12, 12, 8),
                      child: Column(
                        children: currentProfile.serviceNodes
                            .map((node) => buildNodeTile(node))
                            .toList(),
                      ),
                    ),
                  ),
                ),
              ],
            ),
          ),
          if (isInProgress)
            Padding(
              padding: const EdgeInsets.only(top: 8),
              child: LinearProgressIndicator(),
            ),
        ],
      ),
      actions: [
        dialogButton(
          'Cancel',
          onPressed: isInProgress ? null : close,
          isOutline: true,
        ),
        dialogButton(
          'OK',
          onPressed: isInProgress ? null : submit,
        ),
      ],
    );
  });
}

void showServerSettingsWithValue(
    ServerConfig serverConfig,
    OverlayDialogManager dialogManager,
    void Function(VoidCallback)? upSetState) async {
  var isInProgress = false;
  final idCtrl = TextEditingController(text: serverConfig.idServer);
  final relayCtrl = TextEditingController(text: serverConfig.relayServer);
  final apiCtrl = TextEditingController(text: serverConfig.apiServer);
  final keyCtrl = TextEditingController(text: serverConfig.key);

  RxString idServerMsg = ''.obs;
  RxString relayServerMsg = ''.obs;
  RxString apiServerMsg = ''.obs;

  final controllers = [idCtrl, relayCtrl, apiCtrl, keyCtrl];
  final errMsgs = [
    idServerMsg,
    relayServerMsg,
    apiServerMsg,
  ];

  dialogManager.show((setState, close, context) {
    Future<bool> submit() async {
      setState(() {
        isInProgress = true;
      });
      bool ret = await setServerConfig(
          null,
          errMsgs,
          ServerConfig(
              idServer: idCtrl.text.trim(),
              relayServer: relayCtrl.text.trim(),
              apiServer: apiCtrl.text.trim(),
              key: keyCtrl.text.trim()));
      setState(() {
        isInProgress = false;
      });
      return ret;
    }

    Widget buildField(
        String label, TextEditingController controller, String errorMsg,
        {String? Function(String?)? validator, bool autofocus = false}) {
      if (isDesktop || isWeb) {
        return Row(
          children: [
            SizedBox(
              width: 120,
              child: Text(label),
            ),
            SizedBox(width: 8),
            Expanded(
              child: TextFormField(
                controller: controller,
                decoration: InputDecoration(
                  errorText: errorMsg.isEmpty ? null : errorMsg,
                  contentPadding:
                      EdgeInsets.symmetric(horizontal: 8, vertical: 12),
                ),
                validator: validator,
                autofocus: autofocus,
              ).workaroundFreezeLinuxMint(),
            ),
          ],
        );
      }

      return TextFormField(
        controller: controller,
        decoration: InputDecoration(
          labelText: label,
          errorText: errorMsg.isEmpty ? null : errorMsg,
        ),
        validator: validator,
      ).workaroundFreezeLinuxMint();
    }

    return CustomAlertDialog(
      title: Row(
        children: [
          Expanded(child: Text(translate('ID/Relay Server'))),
          ...ServerConfigImportExportWidgets(controllers, errMsgs),
        ],
      ),
      content: ConstrainedBox(
        constraints: const BoxConstraints(minWidth: 500),
        child: Form(
          child: Obx(() => Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  buildField(translate('ID Server'), idCtrl, idServerMsg.value,
                      autofocus: true),
                  SizedBox(height: 8),
                  if (!isIOS && !isWeb) ...[
                    buildField(translate('Relay Server'), relayCtrl,
                        relayServerMsg.value),
                    SizedBox(height: 8),
                  ],
                  buildField(
                    translate('API Server'),
                    apiCtrl,
                    apiServerMsg.value,
                    validator: (v) {
                      if (v != null && v.isNotEmpty) {
                        if (!(v.startsWith('http://') ||
                            v.startsWith("https://"))) {
                          return translate("invalid_http");
                        }
                      }
                      return null;
                    },
                  ),
                  SizedBox(height: 8),
                  buildField('Key', keyCtrl, ''),
                  if (isInProgress)
                    Padding(
                      padding: EdgeInsets.only(top: 8),
                      child: LinearProgressIndicator(),
                    ),
                ],
              )),
        ),
      ),
      actions: [
        dialogButton('Cancel', onPressed: () {
          close();
        }, isOutline: true),
        dialogButton(
          'OK',
          onPressed: () async {
            if (await submit()) {
              close();
              showToast(translate('Successful'));
              upSetState?.call(() {});
            } else {
              showToast(translate('Failed'));
            }
          },
        ),
      ],
    );
  });
}

void setPrivacyModeDialog(
  OverlayDialogManager dialogManager,
  List<TToggleMenu> privacyModeList,
  RxString privacyModeState,
) async {
  dialogManager.dismissAll();
  dialogManager.show((setState, close, context) {
    return CustomAlertDialog(
      title: Text(translate('Privacy mode')),
      content: Column(
          mainAxisAlignment: MainAxisAlignment.spaceEvenly,
          children: privacyModeList
              .map((value) => CheckboxListTile(
                    contentPadding: EdgeInsets.zero,
                    visualDensity: VisualDensity.compact,
                    title: value.child,
                    value: value.value,
                    onChanged: value.onChanged,
                  ))
              .toList()),
    );
  }, backDismiss: true, clickMaskDismiss: true);
}
