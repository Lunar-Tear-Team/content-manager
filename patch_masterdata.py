#!/usr/bin/env python3
"""
Patch master data timestamps to extend content availability.

Decrypts the MasterMemory binary (.bin.e), extends EndDatetime fields from
the 2020-2029 range to 2030-12-31, and re-encrypts with the same AES key/IV.

The MasterMemory format is:
  [msgpack header: Dict<string, (offset, length)>][LZ4-compressed table blobs]
Each table blob is an ExtType(99) containing LZ4-compressed msgpack arrays.
Rows are positional arrays (no field names); column indices are hardcoded
from the entity class definitions in schemas.json.

Requires: pip install pycryptodome msgpack lz4

Usage examples:

  # Default key/IV (built-in), default input path
  python patch_masterdata.py

  # Custom input/output paths
  python patch_masterdata.py \\
      --input original.bin.e --output patched.bin.e

  # Override key/IV via hex strings
  python patch_masterdata.py --key 0123...ff --iv abcd...ef

  # Override key/IV via files (dumped via Frida)
  python patch_masterdata.py --key-file masterdata_key.bin --iv-file masterdata_iv.bin

  # Dry run (decrypt + patch + report, no re-encrypt or write)
  python patch_masterdata.py --dry-run
"""

import argparse
import os
import struct
import sys
from datetime import datetime, timezone

import lz4.block
import msgpack
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import json


DEFAULT_INPUT = os.path.join("server", "assets", "release", "20240404193219.bin.e")
DEFAULT_KEY = "36436230313332314545356536624265"
DEFAULT_IV  = "45666341656634434165356536446141"

# --- Timestamp patching constants ---

TARGET_END_DT = datetime(2030, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
TARGET_END_MS = int(TARGET_END_DT.timestamp() * 1000)
EXPIRED_END_DT = datetime(2020, 1, 1, tzinfo=timezone.utc)
EXPIRED_END_MS = int(EXPIRED_END_DT.timestamp() * 1000)

MIN_PATCH_MS = int(datetime(2010, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
MAX_PATCH_MS = int(datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

SKIP_TABLES = frozenset({"m_omikuji"})

EMPTY_TABLES = frozenset({"m_maintenance"})

# Client has MaxGimmickSequenceSchedule = 1024.  Only patch schedules whose
# StartDatetime (column 1) is before this cutoff so the non-expired count
# stays under the limit.  Through 2023-02 there are 1022 entries (safe).
SCHEDULE_PATCH_CUTOFF = datetime(2023, 2, 1, tzinfo=timezone.utc)
SCHEDULE_PATCH_CUTOFF_MS = int(SCHEDULE_PATCH_CUTOFF.timestamp() * 1000)

# Maps table name -> (column_index, max_value_ms).  Rows whose int64 at
# column_index >= max_value_ms are skipped (EndDatetime left original/expired).
TABLE_PATCH_FILTERS = {
    'm_gimmick_sequence_schedule': (1, SCHEDULE_PATCH_CUTOFF_MS),
}

# Derived from entity class definitions in schemas.json (Key attribute indices).
# Maps snake_case table name -> list of (column_index, field_name) for end-datetime columns.
PATCH_COLUMNS = {
    'm_appeal_dialog': [(5, 'EndDatetime')],
    'm_big_hunt_schedule': [(3, 'ChallengeEndDatetime')],
    'm_cage_ornament': [(2, 'EndDatetime')],
    'm_consumable_item_term': [(2, 'EndDatetime')],
    'm_costume_collection_bonus': [(6, 'EndDatetime')],
    'm_dokan': [(4, 'EndDatetime')],
    'm_enhance_campaign': [(5, 'EndDatetime')],
    'm_event_quest_chapter': [(9, 'EndDatetime')],
    'm_event_quest_daily_group': [(2, 'EndDatetime')],
    'm_event_quest_guerrilla_free_open': [(4, 'EndDatetime')],
    'm_event_quest_labyrinth_season': [(3, 'EndDatetime')],
    'm_event_quest_limit_content': [(6, 'EndDatetime')],
    'm_event_quest_limit_content_deck_restriction': [(4, 'EndDatetime')],
    'm_gacha_medal': [(4, 'AutoConvertDatetime')],
    'm_gimmick_sequence_schedule': [(2, 'EndDatetime')],
    'm_important_item_effect': [(6, 'EndDatetime')],
    'm_login_bonus': [(5, 'EndDatetime'), (6, 'StampReceiveEndDatetime')],
    'm_mission_pass': [(2, 'EndDatetime')],
    'm_mission_term': [(2, 'EndDatetime')],
    'm_mom_banner': [(7, 'EndDatetime')],
    'm_mom_point_banner': [(4, 'EndDatetime')],
    'm_navi_cut_in': [(4, 'EndDatetime')],
    'm_omikuji': [(2, 'EndDatetime')],
    'm_portal_cage_access_point_function_group_schedule': [(5, 'EndDatetime')],
    'm_possession_acquisition_route': [(7, 'EndDatetime')],
    'm_premium_item': [(3, 'EndDatetime')],
    'm_pvp_season': [(3, 'SeasonEndDatetime')],
    'm_quest_bonus_term_group': [(3, 'EndDatetime')],
    'm_quest_campaign': [(4, 'EndDatetime')],
    'm_quest_schedule': [(3, 'EndDatetime')],
    'm_shop': [(10, 'EndDatetime')],
    'm_shop_item_cell_term': [(2, 'EndDatetime')],
    'm_tip': [(6, 'EndDatetime')],
    'm_title_flow_movie': [(3, 'EndDatetime')],
    'm_webview_mission': [(5, 'EndDatetime')],
    'm_webview_panel_mission': [(4, 'EndDatetime')],
}


# --- AES helpers ---

def aes_decrypt(data, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return unpad(cipher.decrypt(data), AES.block_size)


def aes_encrypt(data, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return cipher.encrypt(pad(data, AES.block_size))


# --- MasterMemory binary helpers ---

def read_lz4_ext_header(ext_data):
    """Parse the uncompressed-length prefix from an ExtType(99) payload.

    C# MessagePack writes the original size as a msgpack int before the
    LZ4-compressed bytes.  Returns (uncompressed_length, lz4_bytes).
    """
    tag = ext_data[0]
    if tag == 0xd2:  # int32
        return struct.unpack('>i', ext_data[1:5])[0], ext_data[5:]
    if tag == 0xce:  # uint32
        return struct.unpack('>I', ext_data[1:5])[0], ext_data[5:]
    if tag == 0xd1:  # int16
        return struct.unpack('>h', ext_data[1:3])[0], ext_data[3:]
    if tag == 0xcd:  # uint16
        return struct.unpack('>H', ext_data[1:3])[0], ext_data[3:]
    if tag <= 0x7f:  # positive fixint
        return tag, ext_data[1:]
    raise ValueError(f"Unexpected msgpack tag 0x{tag:02x} in LZ4 ext header")


def build_lz4_ext_blob(decompressed_data):
    """Re-compress and wrap as a msgpack ExtType(99) blob."""
    compressed = lz4.block.compress(decompressed_data, store_size=False)
    header = b'\xd2' + struct.pack('>i', len(decompressed_data))
    return msgpack.packb(msgpack.ExtType(99, header + compressed), use_bin_type=True)


# --- Msgpack binary walker ---
# Navigates raw msgpack bytes to find and patch int64 values at specific
# array positions without deserializing/re-serializing (avoids int64/uint64
# encoding differences between C# and Python msgpack).

def skip_msgpack_value(data, pos):
    """Skip one msgpack value starting at pos, return position after it."""
    tag = data[pos]
    if tag <= 0x7f or tag >= 0xe0:
        return pos + 1
    if 0xa0 <= tag <= 0xbf:
        return pos + 1 + (tag & 0x1f)
    if 0x90 <= tag <= 0x9f:
        n = tag & 0x0f
        p = pos + 1
        for _ in range(n):
            p = skip_msgpack_value(data, p)
        return p
    if 0x80 <= tag <= 0x8f:
        n = tag & 0x0f
        p = pos + 1
        for _ in range(n * 2):
            p = skip_msgpack_value(data, p)
        return p
    FIXED = {
        0xc0: 1, 0xc2: 1, 0xc3: 1,
        0xca: 5, 0xcb: 9,
        0xcc: 2, 0xcd: 3, 0xce: 5, 0xcf: 9,
        0xd0: 2, 0xd1: 3, 0xd2: 5, 0xd3: 9,
        0xd4: 3, 0xd5: 4, 0xd6: 6, 0xd7: 10, 0xd8: 18,
    }
    if tag in FIXED:
        return pos + FIXED[tag]
    LENGTH_PREFIXED = {
        0xc4: (1, 'B'), 0xc5: (2, '>H'), 0xc6: (4, '>I'),  # bin
        0xd9: (1, 'B'), 0xda: (2, '>H'), 0xdb: (4, '>I'),  # str
        0xc7: (1, 'B'), 0xc8: (2, '>H'), 0xc9: (4, '>I'),  # ext (+ type byte)
    }
    if tag in LENGTH_PREFIXED:
        sz_bytes, fmt = LENGTH_PREFIXED[tag]
        n = struct.unpack(fmt, data[pos + 1:pos + 1 + sz_bytes])[0]
        extra = 1 if tag in (0xc7, 0xc8, 0xc9) else 0  # ext type byte
        return pos + 1 + sz_bytes + extra + n
    ARRAY_MAP = {0xdc: (2, '>H'), 0xdd: (4, '>I'), 0xde: (2, '>H'), 0xdf: (4, '>I')}
    if tag in ARRAY_MAP:
        sz_bytes, fmt = ARRAY_MAP[tag]
        n = struct.unpack(fmt, data[pos + 1:pos + 1 + sz_bytes])[0]
        items = n * 2 if tag in (0xde, 0xdf) else n
        p = pos + 1 + sz_bytes
        for _ in range(items):
            p = skip_msgpack_value(data, p)
        return p
    raise ValueError(f"Unknown msgpack tag 0x{tag:02x} at pos {pos}")


def read_array_len(data, pos):
    """Read array header at pos, return (element_count, first_element_pos)."""
    tag = data[pos]
    if 0x90 <= tag <= 0x9f:
        return (tag & 0x0f, pos + 1)
    if tag == 0xdc:
        return (struct.unpack('>H', data[pos + 1:pos + 3])[0], pos + 3)
    if tag == 0xdd:
        return (struct.unpack('>I', data[pos + 1:pos + 5])[0], pos + 5)
    raise ValueError(f"Expected array at pos {pos}, got tag 0x{tag:02x}")


def read_msgpack_int(data, pos):
    """Read integer at pos without advancing."""
    tag = data[pos]
    if 0x00 <= tag <= 0x7f: return tag
    if 0xe0 <= tag <= 0xff: return tag - 0x100
    if tag == 0xcc: return data[pos+1]
    if tag == 0xcd: return struct.unpack('>H', data[pos+1:pos+3])[0]
    if tag == 0xce: return struct.unpack('>I', data[pos+1:pos+5])[0]
    if tag == 0xd0: return struct.unpack('>b', data[pos+1:pos+2])[0]
    if tag == 0xd1: return struct.unpack('>h', data[pos+1:pos+3])[0]
    if tag == 0xd2: return struct.unpack('>i', data[pos+1:pos+5])[0]
    if tag == 0xd3: return struct.unpack('>q', data[pos+1:pos+9])[0]
    return None


def patch_table_blob(blob, col_indices, row_filter=None, active_ids=None):
    """Patch int64 datetime values in-place within a decompressed table blob.

    blob:        bytearray of decompressed msgpack (array of row-arrays)
    col_indices: set of column indices whose values should be patched
    row_filter:  optional (filter_col_index, max_value_ms) — when set, rows
                 whose int64 at filter_col_index >= max_value_ms are skipped
    active_ids:  optional set of valid IDs. If provided and the row's ID (at col 0)
                 is NOT in this set, we forcefully patch EndDatetime to EXPIRED_END_MS (1000).

    Returns (patched_count, skipped_count).
    """
    row_count, pos = read_array_len(blob, 0)
    patched = 0
    skipped = 0
    for _ in range(row_count):
        col_count, p = read_array_len(blob, pos)

        skip_row = False
        row_id = None
        if active_ids is not None and col_count > 0:
            row_id = read_msgpack_int(blob, p)

        if row_filter is not None:
            filter_col, filter_max = row_filter
            fp = p
            for ci in range(min(filter_col + 1, col_count)):
                if ci == filter_col and blob[fp] == 0xd3:
                    val = struct.unpack('>q', blob[fp + 1:fp + 9])[0]
                    if val >= filter_max:
                        skip_row = True
                    break
                fp = skip_msgpack_value(blob, fp)

        if skip_row:
            skipped += 1
            for col_i in range(col_count):
                p = skip_msgpack_value(blob, p)
        else:
            is_active = True
            if active_ids is not None and row_id not in active_ids:
                is_active = False

            for col_i in range(col_count):
                if col_i in col_indices and blob[p] == 0xd3:
                    val = struct.unpack('>q', blob[p + 1:p + 9])[0]
                    if (MIN_PATCH_MS <= val <= MAX_PATCH_MS) or val == TARGET_END_MS or val == EXPIRED_END_MS:
                        patch_val = TARGET_END_MS if is_active else EXPIRED_END_MS
                        struct.pack_into('>q', blob, p + 1, patch_val)
                        patched += 1
                p = skip_msgpack_value(blob, p)
        pos = p
    return patched, skipped


# --- Main ---

def main():
    parser = argparse.ArgumentParser(
        description="Patch master data timestamps to extend content to 2030."
    )

    key_group = parser.add_mutually_exclusive_group()
    key_group.add_argument("--key", default=DEFAULT_KEY,
                           help=f"AES key as hex string (default: built-in)")
    key_group.add_argument("--key-file", help="Path to raw key file (16 or 32 bytes)")

    iv_group = parser.add_mutually_exclusive_group()
    iv_group.add_argument("--iv", default=DEFAULT_IV,
                          help=f"AES IV as hex string (default: built-in)")
    iv_group.add_argument("--iv-file", help="Path to raw IV file (16 bytes)")

    parser.add_argument("--input", default=DEFAULT_INPUT,
                        help=f"Input .bin.e file (default: {DEFAULT_INPUT})")
    parser.add_argument("--output",
                        help="Output .bin.e file (default: overwrite input)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Decrypt and patch but don't write output")
    parser.add_argument("--sync-schedule",
                        help="Path to content_schedule.json to forcefully expire unscheduled content")

    args = parser.parse_args()

    # Load key
    if args.key_file:
        with open(args.key_file, "rb") as f:
            key = f.read()
    else:
        key = bytes.fromhex(args.key)
    if len(key) not in (16, 32):
        print(f"ERROR: AES key must be 16 or 32 bytes, got {len(key)}", file=sys.stderr)
        sys.exit(1)

    # Load IV
    if args.iv_file:
        with open(args.iv_file, "rb") as f:
            iv = f.read()
    else:
        iv = bytes.fromhex(args.iv)
    if len(iv) != 16:
        print(f"ERROR: AES IV must be 16 bytes, got {len(iv)}", file=sys.stderr)
        sys.exit(1)

    aes_bits = len(key) * 8
    output_path = args.output or args.input

    # Read encrypted binary
    print(f"Reading {args.input}...")
    with open(args.input, "rb") as f:
        encrypted = f.read()
    print(f"  Encrypted size: {len(encrypted)} bytes")

    # Decrypt
    print(f"Decrypting (AES-{aes_bits}-CBC)...")
    try:
        decrypted = aes_decrypt(encrypted, key, iv)
    except ValueError as e:
        print(f"ERROR: Decryption failed: {e}", file=sys.stderr)
        print("  Check that the key and IV are correct.", file=sys.stderr)
        sys.exit(1)
    print(f"  Decrypted size: {len(decrypted)} bytes")

    # Parse MasterMemory header: Dict<string, (offset, length)>
    print("Parsing MasterMemory header...")
    try:
        toc = msgpack.unpackb(decrypted, raw=False, strict_map_key=False)
        data_blob = b""
    except msgpack.ExtraData as e:
        toc = e.unpacked
        data_blob = e.extra
    if not isinstance(toc, dict):
        print(f"ERROR: Expected dict header, got {type(toc).__name__}", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(toc)} tables, data blob: {len(data_blob)} bytes")

    # Load dynamic schedule if requested
    schedule_active_ids = {}
    if args.sync_schedule:
        print(f"Loading dynamic schedule from {args.sync_schedule}...")
        try:
            with open(args.sync_schedule) as f:
                schedule = json.load(f)
            index_path = os.path.join(os.path.dirname(args.sync_schedule), "..", "bundle_index.json")
            with open(index_path) as f:
                bundle_index = json.load(f)
            
            allowed_events = set(bundle_index.get("permanent", {}).get("event_chapters", []))
            allowed_gacha = set(bundle_index.get("permanent", {}).get("gacha_ids", []))
            allowed_login = set(bundle_index.get("permanent", {}).get("login_bonuses", []))
            allowed_shops = set(bundle_index.get("permanent", {}).get("shop_ids", []))

            if schedule.get("unreleased_enabled", False):
                allowed_events.update(bundle_index.get("unreleased", {}).get("event_chapters", []))
                allowed_gacha.update(bundle_index.get("unreleased", {}).get("gacha_ids", []))
                allowed_login.update(bundle_index.get("unreleased", {}).get("login_bonuses", []))
                allowed_shops.update(bundle_index.get("unreleased", {}).get("shop_ids", []))

            for bid in schedule.get("active_bundles", []):
                bundle = bundle_index.get("bundles", {}).get(bid)
                if bundle:
                    allowed_events.update(bundle.get("event_chapters", []))
                    allowed_gacha.update(bundle.get("gacha_ids", []))
                    allowed_login.update(bundle.get("login_bonuses", []))
                    allowed_shops.update(bundle.get("shop_ids", []))

            if not allowed_gacha:
                # Failsafe: if no banners are selected, the game freezes. Force the Automata banner (45, 46).
                allowed_gacha.update({45, 46})
                print("  WARNING: 0 gacha banners selected. Enabled fallback banners (45, 46) to prevent client freeze.")

            schedule_active_ids['m_event_quest_chapter'] = allowed_events
            schedule_active_ids['m_mom_banner'] = allowed_gacha
            schedule_active_ids['m_login_bonus'] = allowed_login
            schedule_active_ids['m_shop'] = allowed_shops
            print(f"  Schedule loaded: {len(allowed_events)} events, {len(allowed_gacha)} gachas, {len(allowed_login)} login bonuses, {len(allowed_shops)} shops active")
        except Exception as e:
            print(f"ERROR: Failed to load schedule: {e}", file=sys.stderr)
            sys.exit(1)

    # Patch tables
    print(f"\nPatching EndDatetime fields (target: {TARGET_END_DT.isoformat()})...")
    stats = {}
    total_patched = 0
    new_blobs = {}

    for tname, columns in PATCH_COLUMNS.items():
        if tname in SKIP_TABLES or tname not in toc:
            continue

        offset, length = toc[tname]
        blob = data_blob[offset:offset + length]

        ext = msgpack.unpackb(blob, raw=True)
        if not isinstance(ext, msgpack.ExtType) or ext.code != 99:
            print(f"  WARNING: {tname} is not LZ4-compressed (ExtType 99), skipping")
            continue

        uncompressed_len, lz4_data = read_lz4_ext_header(ext.data)
        decompressed = bytearray(lz4.block.decompress(lz4_data, uncompressed_size=uncompressed_len))

        col_indices = {idx for idx, _ in columns}
        row_filter = TABLE_PATCH_FILTERS.get(tname)
        active_ids = schedule_active_ids.get(tname)

        count, skip_count = patch_table_blob(decompressed, col_indices, row_filter, active_ids)

        if count > 0:
            new_blobs[tname] = build_lz4_ext_blob(bytes(decompressed))
            stats[tname] = (count, skip_count)
            total_patched += count

    print(f"\n  Patched {total_patched} values across {len(stats)} tables:")
    for tname in sorted(stats):
        count, skip_count = stats[tname]
        cols = ", ".join(name for _, name in PATCH_COLUMNS[tname])
        suffix = f" (skipped {skip_count} rows by filter)" if skip_count else ""
        print(f"    {tname}: {count} values ({cols}){suffix}")

    emptied = []
    for tname in sorted(EMPTY_TABLES):
        if tname in toc:
            new_blobs[tname] = msgpack.packb([], use_bin_type=True)
            emptied.append(tname)

    if emptied:
        print(f"\n  Emptied tables: {', '.join(emptied)}")

    if SKIP_TABLES:
        print(f"\n  Skipped tables: {', '.join(sorted(SKIP_TABLES))}")

    if args.dry_run:
        print("\n[DRY RUN] Skipping rebuild and encryption.")
        return

    # Rebuild the data blob: reassemble table blobs at new offsets
    print("\nRebuilding MasterMemory binary...")
    sorted_tables = sorted(toc.items(), key=lambda kv: kv[1][0])

    new_toc = {}
    blob_parts = []
    current_offset = 0

    for tname, (orig_offset, orig_length) in sorted_tables:
        if tname in new_blobs:
            part = new_blobs[tname]
        else:
            part = data_blob[orig_offset:orig_offset + orig_length]
        new_toc[tname] = (current_offset, len(part))
        blob_parts.append(part)
        current_offset += len(part)

    new_data_blob = b''.join(blob_parts)
    new_header = msgpack.packb(new_toc, use_bin_type=True)
    new_decrypted = new_header + new_data_blob
    print(f"  Header: {len(new_header)} bytes, blob: {len(new_data_blob)} bytes")
    print(f"  Total: {len(new_decrypted)} bytes (original: {len(decrypted)})")

    # Re-encrypt
    print(f"Re-encrypting (AES-{aes_bits}-CBC)...")
    re_encrypted = aes_encrypt(new_decrypted, key, iv)
    print(f"  Re-encrypted size: {len(re_encrypted)} bytes")

    # Write output
    print(f"Writing {output_path}...")
    with open(output_path, "wb") as f:
        f.write(re_encrypted)
    print(f"  Done! Patched binary written to {output_path}")


if __name__ == "__main__":
    main()
