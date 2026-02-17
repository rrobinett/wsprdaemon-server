# Column Mapping: spots (live) vs spots_2025 (migrated)

## Likely Mappings

| spots (live)             | spots_2025 (migrated)            | Notes                      |
|--------------------------|----------------------------------|----------------------------|
| time                     | timestamp                        | DateTime field             |
| band                     | band                             | Band in meters             |
| rx_sign                  | real_receiver_call_sign          | Receiver callsign          |
| rx_id                    | receiver_name                    | Receiver ID/name           |
| rx_loc                   | real_receiver_grid               | Receiver grid              |
| rx_lat                   | rx_lat                           | Receiver latitude          |
| rx_lon                   | rx_lon                           | Receiver longitude         |
| rx_az                    | rx_az                            | Receiver azimuth           |
| tx_call                  | spot_call                        | Transmitter callsign       |
| tx_grid                  | spot_grid                        | Transmitter grid           |
| tx_dBm                   | spot_pwr                         | Transmit power             |
| tx_lat                   | tx_lat                           | Transmitter latitude       |
| tx_lon                   | tx_lon                           | Transmitter longitude      |
| tx_az                    | tx_az                            | Transmitter azimuth        |
| SNR                      | spot_snr                         | Signal-to-noise ratio      |
| freq                     | spot_freq                        | Frequency                  |
| drift                    | spot_drift                       | Frequency drift            |
| dt                       | spot_dt                          | Time delta                 |
| km                       | km                               | Distance                   |
| c2_noise                 | wspr_cycle_fft_noise             | FFT noise level            |
| rms_noise                | wspr_cycle_rms_noise             | RMS noise level            |
| sync_quality             | spot_sync_quality                | Sync quality               |
| decode_cycles            | spot_cycles                      | Decode cycles              |
| jitter                   | spot_jitter                      | Jitter                     |
| blocksize                | spot_blocksize                   | Block size                 |
| metric                   | spot_metric                      | Metric                     |
| osd_decode               | spot_decodetype                  | Decode type                |
| nhardmin                 | spot_nhardmin                    | Hard minimum               |
| ipass                    | spot_ipass                       | Pass number                |
| mode                     | spot_pkt_mode                    | Packet mode (W2, F2, etc)  |
| ov_count                 | wspr_cycle_kiwi_overloads_count  | Overload count             |
| v_lat                    | v_lat                            | ? latitude                 |
| v_lon                    | v_lon                            | ? longitude                |
| seqnum                   | -                                | NOT IN spots_2025          |
| running_jobs             | -                                | NOT IN spots_2025          |
| receiver_descriptions    | -                                | NOT IN spots_2025          |

## Fields only in spots_2025

| Column                   | Purpose                |
|--------------------------|------------------------|
| spot_date                | Date string (YYMMDD)   |
| spot_time                | Time string (HHMM)     |
| source_file              | Source tbz filename    |
| tar_file                 | Source tar filename    |
| proxy_upload_this_spot   | Upload flag            |

## Summary

- **Live table (spots)**: Focused on runtime data with seqnum, running_jobs
- **Migrated table (spots_2025)**: Focused on archive data with source file tracking
- **Core data**: All essential spot information is present in both tables
- **Compatible**: Yes, data can be mapped between them

