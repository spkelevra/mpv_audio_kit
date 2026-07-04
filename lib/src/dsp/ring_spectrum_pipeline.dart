// Copyright © 2026 & onwards, Alessandro Di Ronza <ales.drnz@gmail.com>.
// All rights reserved.
// Use of this source code is governed by BSD 3-Clause license that can be found in the LICENSE file.

import 'dart:async';

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
/// Typical usage — wire it to a filter-tap stream:
///
/// ```dart
/// final pipeline = RingSpectrumPipeline();
/// // Feed PCM from any PcmFrame source (filter tap, custom ring, …).
/// player.stream.tap(AudioEffect.equalizer, side: TapSide.post)
///     .listen(pipeline.feed);
///
/// // Consume FFT frames at the upstream's native rate.
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

  /// Replaces the pipeline configuration. Reallocates FFT / window /
  /// EMA memory only on changes that require it.
  void setSettings(SpectrumSettings next) {
    _settings = next;
    _processor.setSettings(next);
    if (!_settingsCtrl.isClosed) _settingsCtrl.add(next);
  }

  /// Feed a PCM frame into the pipeline. Call this from an upstream
  /// [PcmFrame] stream subscription to drive FFT computation.
  ///
  /// Returns immediately — FFT runs synchronously inside [BandProcessor].
  void feed(PcmFrame frame) {
    if (_disposed) return;

    // Passthrough raw PCM.
    if (!_pcmCtrl.isClosed && _pcmCtrl.hasListener) {
      _pcmCtrl.add(frame);
    }

    // Compute FFT and emit.
    if (!_fftCtrl.isClosed && _fftCtrl.hasListener) {
      final fft = _processor.process(frame);
      if (fft != null) _fftCtrl.add(fft);
    }
  }

  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    if (!_fftCtrl.isClosed) await _fftCtrl.close();
    if (!_pcmCtrl.isClosed) await _pcmCtrl.close();
    if (!_settingsCtrl.isClosed) await _settingsCtrl.close();
  }
}
