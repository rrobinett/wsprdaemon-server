# Extracting and Formatting Squarespace DNS Records

Since Squarespace doesn't provide DNS record sorting in their UI, this procedure extracts DNS records via the browser console and formats them using bash.

## One-Time Setup (Safari)

Enable Safari developer tools:

1. Safari → Settings → Advanced
2. Check "Show features for web developers"

## Extraction Process

### Step 1: Extract Data in Safari

1. Navigate to your Squarespace DNS records page
2. Open JavaScript console: **⌥⌘C** (Option-Command-C)
3. Paste this JavaScript code and press Enter:

```javascript
copy(Array.from(document.querySelectorAll('tr'))
  .map(row => row.innerText)
  .filter(text => !text.includes('HOST') && text.trim())
  .map(text => text.replace(/↵+/g, '\t').trim())
  .sort()
  .join('\n'))
```

This extracts all DNS records, filters out headers, sorts alphabetically, and copies to clipboard.

### Step 2: Format Data in Terminal

Open Terminal and run:

```bash
# Save extracted data from clipboard
pbpaste > dns_records.txt

# Combine fields into rows, sort, and format into aligned columns
paste -d$'\t' - - - - - < dns_records.txt | sort | column -t -s$'\t' > dns_formatted.txt

# View results
cat dns_formatted.txt
```

## Quick One-Liner

After running the JavaScript in Safari console:

```bash
pbpaste > dns_records.txt && paste -d$'\t' - - - - - < dns_records.txt | sort | column -t -s$'\t'
```

## What Each Command Does

- `pbpaste` - Retrieves data from macOS clipboard
- `paste -d$'\t' - - - - -` - Combines every 5 lines into one tab-separated row (HOST, TYPE, PRIORITY, TTL, DATA)
- `sort` - Sorts records alphabetically by hostname
- `column -t -s$'\t'` - Formats into aligned columns

## Output Format

The final output will be a sorted table with aligned columns:

```
_domainconnect    CNAME  0    4 hrs  _domainconnect.domains.squarespace.com
@                 A      N/A  4 hrs  167.99.168.121
active            A      N/A  1 hr   69.27.184.58
wd0               A      N/A  4 hrs  167.99.168.121
wd1               A      N/A  4 hrs  64.62.200.181
www               CNAME  N/A  4 hrs  wsprdaemon.org
```

## Getting Just Hostnames

To extract only the sorted list of hostnames:

```bash
paste -d$'\t' - - - - - < dns_records.txt | cut -f1 | sort
```

Or save to file:

```bash
paste -d$'\t' - - - - - < dns_records.txt | cut -f1 | sort > hostnames.txt
```

---

*Last updated: November 2025*
