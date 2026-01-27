import 'dart:async';
import 'dart:io';

import 'package:flutter_soloud/flutter_soloud.dart';
import 'package:logging/logging.dart';
import 'package:rxdart/rxdart.dart';

import 'soloud_audio_player.dart';

/// A non-singleton version of [SoloudAudioPlayer] focused on playing assets and local files
/// efficiently using SoLoud's native loaders.
///
/// Supports multiple instances for simultaneous playback (e.g. BGM + SFX).
class SoloudAudioPlayerInstances {
  SoloudAudioPlayerInstances() {
    _seekSubject.stream.debounceTime(const Duration(milliseconds: 500)).listen((
      position,
    ) async {
      final source = currentSource;
      if (source != null) {
        _seek(position);
      }
    });
  }

  final log = Logger('SoloudAudioPlayerInstances');

  // --- Streams ---
  final _processingStateController = BehaviorSubject.seeded(
    ProcessingState.idle,
  );
  final _isPlayingController = BehaviorSubject.seeded(false);
  final _positionController = BehaviorSubject.seeded(Duration.zero);
  final _bufferedPositionController = BehaviorSubject.seeded(Duration.zero);
  final _durationController = BehaviorSubject.seeded(Duration.zero);
  final _currentSourceController = BehaviorSubject<AppAudioSource?>.seeded(
    null,
  );
  final _seekSubject = PublishSubject<Duration>();
  final _errorController = PublishSubject<Object>();

  Stream<ProcessingState> get processingStateStream =>
      _processingStateController.stream;
  Stream<bool> get playingStream => _isPlayingController.stream;
  Stream<Duration> get positionStream => _positionController.stream;
  Stream<Duration> get bufferedPositionStream =>
      _bufferedPositionController.stream;
  Stream<Duration> get durationStream => _durationController.stream;
  Stream<AppAudioSource?> get currentSourceStream =>
      _currentSourceController.stream;
  Stream<Object> get errorStream => _errorController.stream;

  ProcessingState get processingState => _processingStateController.value;
  bool get playing => _isPlayingController.value;
  Duration get position => _positionController.value;
  Duration get duration => _durationController.value;
  AppAudioSource? get currentSource => _currentSourceController.value;

  // --- Internal State ---
  SoLoud get _soloud => SoLoud.instance;
  AudioSource? _audioSource;
  SoundHandle? _soundHandle;
  Timer? _stateTicker;

  // Local settings
  double _volume = 1.0;
  bool _looping = false;

  /// Ensures the SoLoud engine is initialized.
  Future<void> init() async {
    // Usually SoLoud.instance.init() is called once globally.
    // We can check if it's initialized or just call it (it handles idempotency usually).
    if (!_soloud.isInitialized) {
      await _soloud.init();
    }
  }

  /// Sets the volume (0.0 to 1.0).
  void setVolume(double volume) {
    _volume = volume.clamp(0.0, 1.0);
    if (_soundHandle != null && _soloud.getIsValidVoiceHandle(_soundHandle!)) {
      _soloud.setVolume(_soundHandle!, _volume);
    }
  }

  /// Sets whether the audio should loop.
  void setLooping(bool looping) {
    _looping = looping;
    if (_soundHandle != null && _soloud.getIsValidVoiceHandle(_soundHandle!)) {
      _soloud.setLooping(_soundHandle!, _looping);
    }
  }

  /// Loads and plays an asset from the Flutter bundle.
  Future<void> setAsset(String path, {bool preload = false}) async {
    final source = AppAudioSource(
      uri: path,
      id: path.hashCode.toString(),
      isLocal: true,
    );
    await setSource(source, preload: preload);
  }

  /// Loads and plays a local file.
  Future<void> playFromFile(File file) async {
    final source = AppAudioSource(
      uri: file.path,
      id: file.path.hashCode.toString(),
      isLocal: true,
    );
    await setSource(source);
  }

  /// Loads a source. For this class, it must be a local file or asset.
  Future<void> setSource(AppAudioSource source, {bool preload = false}) async {
    log.info('setSource: ${source.uri}');
    await stop(); // Stop current playback

    _currentSourceController.add(source);
    _processingStateController.add(ProcessingState.loading);

    try {
      if (File(source.uri).existsSync()) {
        _audioSource = await _soloud.loadFile(source.uri);
      } else {
        // Assume asset
        try {
          _audioSource = await _soloud.loadAsset(source.uri);
        } catch (e) {
          log.warning("Failed to load asset: ${source.uri}. Error: $e");
          // Try loading as file one last time if asset failed (maybe absolute path to non-existent file?)
          rethrow;
        }
      }

      // Get duration
      final length = _soloud.getLength(_audioSource!);
      _durationController.add(length);

      _processingStateController.add(ProcessingState.ready);

      if (!preload) {
        await play();
      }
    } catch (e) {
      log.severe("Error loading source: $e");
      _errorController.add(e);
      _processingStateController.add(ProcessingState.idle);
    }
  }

  Future<void> play() async {
    if (_audioSource == null) return;

    // Resume if paused
    if (_soundHandle != null && _soloud.getIsValidVoiceHandle(_soundHandle!)) {
      if (_soloud.getPause(_soundHandle!)) {
        _soloud.setPause(_soundHandle!, false);
        _isPlayingController.add(true);
      }
      return;
    }

    try {
      // Play new instance
      _soundHandle = await _soloud.play(_audioSource!);

      // Apply settings immediately
      _soloud.setVolume(_soundHandle!, _volume);
      _soloud.setLooping(_soundHandle!, _looping);

      _isPlayingController.add(true);
      _startTicker();
    } catch (e) {
      log.severe("Error playing: $e");
      _errorController.add(e);
    }
  }

  Future<void> pause() async {
    if (_soundHandle != null && _soloud.getIsValidVoiceHandle(_soundHandle!)) {
      _soloud.setPause(_soundHandle!, true);
      _isPlayingController.add(false);
    }
  }

  Future<void> seek(Duration position) async {
    if (_soundHandle != null && _soloud.getIsValidVoiceHandle(_soundHandle!)) {
      _seek(position);
    }
  }

  Future<void> _seek(Duration position) async {
    if (_soundHandle != null) {
      _soloud.seek(_soundHandle!, position);
      _positionController.add(position);
    }
  }

  Future<void> stop() async {
    _stateTicker?.cancel();
    if (_soundHandle != null) {
      try {
        await _soloud.stop(_soundHandle!);
      } catch (_) {}
      _soundHandle = null;
    }

    if (_audioSource != null) {
      try {
        await _soloud.disposeSource(_audioSource!);
      } catch (_) {}
      _audioSource = null;
    }

    _processingStateController.add(ProcessingState.idle);
    _isPlayingController.add(false);
    _positionController.add(Duration.zero);
    _bufferedPositionController.add(Duration.zero);
  }

  Future<void> dispose() async {
    await stop();
    await _processingStateController.close();
    await _isPlayingController.close();
    await _positionController.close();
    await _bufferedPositionController.close();
    await _durationController.close();
    await _currentSourceController.close();
    await _seekSubject.close();
    await _errorController.close();
  }

  void _startTicker() {
    _stateTicker?.cancel();
    _stateTicker = Timer.periodic(const Duration(milliseconds: 200), (_) {
      if (_soundHandle == null) return;

      if (_soloud.getIsValidVoiceHandle(_soundHandle!)) {
        final currentPos = _soloud.getPosition(_soundHandle!);
        _positionController.add(currentPos);

        // Sync pause state just in case
        final isPaused = _soloud.getPause(_soundHandle!);
        if (isPaused != !_isPlayingController.value) {
          _isPlayingController.add(!isPaused);
        }

        log.info("Current position: $currentPos - isPlaying: ${!isPaused}");
      } else {
        // Handle finished
        if (_processingStateController.value != ProcessingState.completed &&
            _processingStateController.value != ProcessingState.idle) {
          _processingStateController.add(ProcessingState.completed);
          _isPlayingController.add(false);
          _stateTicker?.cancel();
        }
      }
    });
  }
}
