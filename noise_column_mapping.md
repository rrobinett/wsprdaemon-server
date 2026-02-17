# Column Mapping: noise (live) vs noise_2025 (migrated)

## Likely Mappings

| noise (live)             | noise_2025 (migrated)   | Notes                      |
|--------------------------|-------------------------|----------------------------|
| time                     | time                    | DateTime field             |
| site                     | site                    | Site/station callsign      |
| receiver                 | receiver                | Receiver ID/name           |
| rx_loc                   | rx_loc                  | Receiver grid              |
| band                     | band                    | Band (as string)           |
| rms_level                | rms_level               | RMS noise level            |
| c2_level                 | c2_level                | C2/FFT noise level         |
| ov                       | ov                      | Overload count             |
| seqnum                   | -                       | NOT IN noise_2025          |
| running_jobs             | -                       | NOT IN noise_2025          |
| receiver_descriptions    | -                       | NOT IN noise_2025          |
| rx_sign                  | -                       | ALIAS for site             |
| rx_id                    | -                       | ALIAS for receiver         |

## Fields only in noise_2025

| Column                   | Purpose                |
|--------------------------|------------------------|
| tar_file                 | Source tar filename    |
| source_file              | Source tbz filename    |

## Summary

- **Live table (noise)**: Has runtime metadata (seqnum, running_jobs, receiver_descriptions)
- **Migrated table (noise_2025)**: Has archive tracking (tar_file, source_file)
- **Core data**: All essential noise measurements are identical (time, site, receiver, rx_loc, band, rms_level, c2_level, ov)
- **Compatible**: YES - schemas are highly compatible, only metadata differs

