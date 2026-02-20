#!/bin/bash
# dedup-tar-files.sh  v1.1
#
# Reads a list of tar file paths (one per line), identifies unique filenames,
# hard-links one copy of each into DEST_DIR, and records duplicates to a log.
#
# Uniqueness is determined by basename only (same name = same file).
# Files are NOT checksummed - assumes same name => same content.
#
# Usage:
#   ./dedup-tar-files.sh <find-tars.log> [dest_dir]
#
# Outputs:
#   DEST_DIR/                    -- hard links to one copy of each unique tar
#   duplicates.log               -- list of duplicate paths and their canonical copy
#   dedup-summary.log            -- summary of what was done

VERSION="1.1"
INPUT="${1:-find-tars.log}"
DEST_DIR="${2:-/srv/wd_archive/wd0-tar-files}"
DUPES_LOG="./duplicates.log"
SUMMARY_LOG="./dedup-summary.log"

if [[ ! -f "${INPUT}" ]]; then
    echo "ERROR: input file not found: ${INPUT}"
    echo "Usage: $0 <find-tars.log> [dest_dir]"
    exit 1
fi

echo "========================================"
echo "dedup-tar-files.sh  v${VERSION}"
echo "========================================"
echo "Input     : ${INPUT}"
echo "Dest dir  : ${DEST_DIR}"
echo "Dupes log : ${DUPES_LOG}"
echo ""

mkdir -p "${DEST_DIR}"
> "${DUPES_LOG}"
> "${SUMMARY_LOG}"

declare -A canonical   # basename -> first (canonical) full path
LINKED=0
DUPES=0
MISSING=0

while IFS= read -r filepath; do
    # Skip blank lines and comments
    [[ -z "${filepath}" || "${filepath}" =~ ^# ]] && continue

    basename="${filepath##*/}"

    if [[ ! -f "${filepath}" ]]; then
        echo "MISSING  ${filepath}"
        echo "MISSING  ${filepath}" >> "${SUMMARY_LOG}"
        MISSING=$(( MISSING + 1 ))
        continue
    fi

    if [[ -z "${canonical[$basename]+_}" ]]; then
        # First time we've seen this filename - this is the canonical copy
        canonical[$basename]="${filepath}"
        dest="${DEST_DIR}/${basename}"

        if [[ -e "${dest}" ]]; then
            echo "EXISTS   ${dest} (already linked, skipping)"
            echo "EXISTS   ${dest}" >> "${SUMMARY_LOG}"
        else
            size=$(du -sh "${filepath}" | cut -f1)
            echo "LINK     ${basename}  (${size})  <- ${filepath}"
            ln "${filepath}" "${dest}"
            echo "LINK     ${dest} <- ${filepath}" >> "${SUMMARY_LOG}"
            LINKED=$(( LINKED + 1 ))
        fi
    else
        # Duplicate - record it
        canon="${canonical[$basename]}"
        size=$(du -sh "${filepath}" | cut -f1)
        echo "DUPE     ${basename}  (${size})  -- canonical: ${canon}"
        echo "${filepath}  DUPLICATE_OF  ${canon}" >> "${DUPES_LOG}"
        DUPES=$(( DUPES + 1 ))
    fi

done < "${INPUT}"

# Write summary
{
    echo "========================================"
    echo "dedup-tar-files.sh  v${VERSION}"
    echo "Completed: $(date)"
    echo "========================================"
    echo "Input          : ${INPUT}"
    echo "Dest dir       : ${DEST_DIR}"
    echo "Linked (unique): ${LINKED}"
    echo "Duplicates     : ${DUPES}"
    echo "Missing        : ${MISSING}"
    echo ""
    echo "To remove duplicates after verification:"
    echo "  awk '{print \$1}' ${DUPES_LOG} | xargs rm -v"
} | tee -a "${SUMMARY_LOG}"

echo ""
echo "Dest dir contents:"
ls -lh "${DEST_DIR}" | sort -k9

echo ""
echo "Done. Duplicates list: ${DUPES_LOG}"
