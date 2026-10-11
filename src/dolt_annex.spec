# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_submodules

a = Analysis(
    ['dolt_annex/__main__.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('dolt_annex/data', 'dolt_annex/data'),
        ('dolt_annex/gallery_dl_plugin/skip.sqlite3', 'dolt_annex/gallery_dl_plugin'),
        ('dolt_annex/gallery_dl_plugin/gallery_dl_config.json', 'dolt_annex/gallery_dl_plugin')
    ],
    # gallery-dl uses a ton of dynamic imports. It's easiest to simply bundle all of its modules.
    hiddenimports=collect_submodules('gallery_dl'),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='dolt-annex',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='interactive',
)