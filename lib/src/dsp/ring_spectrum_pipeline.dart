// Copyright © 2026 & onwards, Alessandro Di Ronza <ales.drnz@gmail.com>.
// All rights reserved.
// Use of this source code is governed by BSD 3-Clause license that can be found in the LICENSE file.

import 'dart:async';

import 'package:flutter/foundation.dart';
import '../models/fft_frame.dart';
import '../models/pcm_frame.dart';
import '../types/settings/spectrum_settings.dart';
import 'band_processor.dart';

/// Real-time FFT pipeline that reads PCM from a [PcmFrame] stream
/// (typically the filter-tap ring buffer) instead of polling
/// `pcm-tap-frame`.
///
/// Unlike [SpectrumPipeline], which blocks on each `mpv_get_property`
/// call waiting for fresh AO samples, this pipeline receives PCM
/// frames pushed by an upstream source — the caller owns the tap and
/// simply forwards every frame. The FFT runs in Dart via
/// [BandProcessor], so emit rate is driven entirely by how fast the
/// upstream delivers PCM, not by FFI round-trip latency.
///
/// **Emit throttling**: even though PCM arrives at the filter-tap's
/// native rate (often 40–90+ Hz), FFT frames are only emitted to
/// [fftStream] at [SpectrumSettings.emitInterval]. The internal
/// [BandProcessor] still processes every PCM frame so its EMA smoothing
/// stays accurate — we just snapshot the bands less frequently. This
/// prevents overwhelming Flutter's render pipeline with more repaints
/// than the display can show.
///
/// Typical usage — wire it to a filter-tap stream:
///
/// ```dart
/// final pipeline = RingSpectrumPipeline();
/// // Feed PCM from any PcmFrame source (filter tap, custom ring, …).
/// player.stream.tap(AudioEffect.equalizer, side: TapSide.post)
///     .listen(pipeline.feed);
///
/// // Consume FFT frames at the configured emitInterval rate.
/// pipeline.fftStream.listen((frame) { /* paint */ });
/// ```
class RingSpectrumPipeline {
  /// Creates a ring-backed spectrum pipeline with [settings].
  RingSpectrumPipeline([SpectrumSettings settings = SpectrumSettings.defaults])
      : _settings = settings {
    _fftCtrl = StreamController<FftFrame>.broadcast();
    _pcmCtrl = StreamController<PcmFrame>.broadcast();
    // Settings stream is always listenable (config-only, not lazy).
    _settingsCtrl = StreamController<SpectrumSettings>.broadcast();
  }

  late final StreamController<FftFrame> _fftCtrl;
  late final StreamController<PcmFrame> _pcmCtrl;
  late final StreamController<SpectrumSettings> _settingsCtrl;

  /// Broadcast stream of FFT frames computed from fed PCM.
  Stream<FftFrame> get fftStream => _fftCtrl.stream;

  /// Broadcast stream of raw PCM frames (passthrough of [feed]).
  Stream<PcmFrame> get pcmStream => _pcmCtrl.stream;

  /// Broadcast stream of settings changes.
  Stream<SpectrumSettings> get settingsStream => _settingsCtrl.stream;

  SpectrumSettings _settings;
  SpectrumSettings get settings => _settings;

  bool _disposed = false;

  // Shared FFT / windowing / EMA pipeline — same component the public
  // [BandProcessor] exposes, so ring-backed and pcm-tap-backed spectrum
  // surfaces stay byte-identical when fed equivalent PCM.
  late final BandProcessor _processor = BandProcessor(_settings);

  /// Throttle: emit FftFrame to consumers at this interval.
  /// PCM is always processed through [BandProcessor] for accurate EMA,
  /// but the snapshot is taken at most once per [emitInterval].
  Timer? _emitTimer;

  /// Replaces the pipeline configuration. Reallocates FFT / window /
  /// EMA memory only on changes that require it. Cancels and restarts
  /// the emit timer when [SpectrumSettings.emitInterval] changes.
  void setSettings(SpectrumSettings next) {
    final emitChanged = next.emitInterval != _settings.emitInterval;
    _settings = next;
    _processor.setSettings(next);
    if (!_settingsCtrl.isClosed) _settingsCtrl.add(next);

    if (emitChanged && _fftCtrl.hasListener) {
      _emitTimer?.cancel();
      _startEmitTimer();
    }
  }

  /// Start the periodic emit timer that snapshots bands at [emitInterval].
  void _startEmitTimer() {
    _emitTimer = Timer.periodic(_settings.emitInterval, (_) => _tryEmit());
  }

  /// Stop the emit timer.
  void _stopEmitTimer() {
    _emitTimer?.cancel();
    _emitTimer = null;
  }

  /// Snapshot the latest bands from [BandProcessor] and emit to
  /// [fftStream]. Called periodically by [_emitTimer].
  void _tryEmit() {
    if (_disposed || _fftCtrl.isClosed) return;
    final frame = _processor.snapshot();
    if (frame != null && _fftCtrl.hasListener) {
      _fftCtrl.add(frame);
    }
  }

  // Feed-rate diagnostics.
  int _feedCount = 0;
  DateTime? _feedRateCheckTime;

  /// Feed a PCM frame into the pipeline. Call this from an upstream
  /// [PcmFrame] stream subscription to drive FFT computation.
  ///
  /// Returns immediately — FFT runs synchronously inside [BandProcessor].
  /// The FFT result is accumulated internally; actual emissions to
  /// [fftStream] are throttled by [SpectrumSettings.emitInterval].
  void feed(PcmFrame frame) {
    if (_disposed) return;

    // Measure feed rate every second (lightweight).
    _feedCount++;
    if (_feedRateCheckTime == null ||
        DateTime.now().difference(_feedRateCheckTime!).inMilliseconds >= 1000) {
      if (_feedRateCheckTime != null) {
        debugPrint('RingSpectrumPipeline feed rate: ${_feedCount} pcm-frames/sec');
      }
      _feedCount = 0;
      _feedRateCheckTime = DateTime.now();
    }

    // Passthrough raw PCM.
    if (!_pcmCtrl.isClosed && _pcmCtrl.hasListener) {
      _pcmCtrl.add(frame);
    }

    // Start emit timer on first feed when there are FFT listeners.
    if (_emitTimer == null && !_fftCtrl.isClosed && _fftCtrl.hasListener) {
      _startEmitTimer();
    }

    // Always process through BandProcessor — keeps EMA smoothing accurate
    // regardless of emit rate. The result is stored internally and will be
    // snapshot by [_tryEmit] at the configured interval.
    _processor.process(frame);
  }

  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _stopEmitTimer();
    if (!_fftCtrl.isClosed) await _fftCtrl.close();
    if (!_pcmCtrl.isClosed) await _pcmCtrl.close();
    if (!_settingsCtrl.isClosed) await _settingsCtrl.close();
  }
}
