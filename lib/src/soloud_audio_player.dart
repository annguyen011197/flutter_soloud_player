import 'dart:async';
import 'dart:io';

import 'package:ffmpeg_kit_flutter_new_https/ffmpeg_kit.dart';
import 'package:ffmpeg_kit_flutter_new_https/ffmpeg_kit_config.dart';
import 'package:ffmpeg_kit_flutter_new_https/ffprobe_kit.dart';
import 'package:flutter_soloud/flutter_soloud.dart';
import 'package:logging/logging.dart';
import 'package:path_provider/path_provider.dart';
import 'package:rxdart/rxdart.dart';

/// Enum mimicking just_audio's ProcessingState
enum ProcessingState {
  /// The player has not loaded an audio source.
  idle,

  /// The player is loading the audio source and is not yet ready to play.
  loading,

  /// The player is buffering data and is not yet able to play.
  buffering,

  /// The player has enough data to play.
  ready,

  /// The player has reached the end of the audio.
  completed,
}

/// State of the audio data stream from FFmpeg
enum StreamDataState {
  /// No stream active
  idle,

  /// Stream is currently loading (FFmpeg is pumping data)
  buffering,

  /// Stream has finished loading (EOF from FFmpeg)
  fullyLoaded,
}

class AppAudioSource {
  AppAudioSource({required this.uri, required this.id, this.isLocal = false});

  final String uri;
  final String id;
  final bool isLocal;
}

class SoloudAudioPlayer {
  SoloudAudioPlayer._() {
    _seekSubject.stream.debounceTime(const Duration(milliseconds: 500)).listen((
      position,
    ) async {
      final source = currentSource;
      if (source != null) {
        _seek(source, startOffset: position);
      }
    });
  }
  static final SoloudAudioPlayer instance = SoloudAudioPlayer._();

  final log = Logger('SoloudAudioPlayer');

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

  // --- Internal Config ---
  static const int _sampleRate = 44100;
  static const int _channels = 2;
  static const int _bytesPerSample = 2; // s16le = 16 bit = 2 bytes
  static const int _bytesPerSecond =
      _sampleRate * _channels * _bytesPerSample; // 176400

  // --- Internal State ---
  SoLoud get _soloud => SoLoud.instance;
  String? _outputPipe;
  AudioSource? _audioSource;
  SoundHandle? _soundHandle;

  StreamDataState _streamDataState = StreamDataState.idle;
  int _bytesBuffered = 0; // Total bytes read from FFmpeg
  Duration _seekOffset =
      Duration.zero; // Tracks where we started the stream (for seeking)

  Timer? _stateTicker;
  Completer<void>? _ffmpegReadyCompleter;
  bool _isFfmpegFinished = false;

  // --- Public Methods ---
  Future<void> setEqualizer(List<double> gains) async {
    if (gains.length != 8) {
      log.warning('Equalizer requires exactly 8 bands');
      return;
    }
    final filters = _soloud.filters;
    if (filters.equalizerFilter.isActive == false) {
      filters.equalizerFilter.activate();
    }
    final list = [
      filters.equalizerFilter.band1,
      filters.equalizerFilter.band2,
      filters.equalizerFilter.band3,
      filters.equalizerFilter.band4,
      filters.equalizerFilter.band5,
      filters.equalizerFilter.band6,
      filters.equalizerFilter.band7,
      filters.equalizerFilter.band8,
    ];
    for (int i = 0; i < 8; i++) {
      final band = list[i];
      final gain = gains[i];
      if (band.value != gain) {
        log.info("Setting band $i from ${band.value} to $gain");
        band.fadeFilterParameter(to: gain, time: Duration(milliseconds: 200));
      }
    }
  }

  Future<void> init() async {
    log.info('Initializing SoloudAudioPlayer');
    await _soloud.init();
    // Enable Equalizer Filter globally (index 0)
    // _soloud.setGlobalFilter(FilterType.eq, FilterType.eq);
    // Warmup FFmpeg
    await FFmpegKit.executeAsync("--version");
  }

  /// Internal callback for the current session
  Future<void> Function(AppAudioSource source)? _onCachedCallback;

  /// Loads and plays the video stream URL
  Future<void> setUrl(
    String url, {
    bool preload = false,
    Future<void> Function(AppAudioSource source)? onCached,
  }) async {
    final source = AppAudioSource(uri: url, id: url.hashCode.toString());
    await setSource(source, preload: preload, onCached: onCached);
  }

  Future<void> playFromFile(File file) async {
    final source = AppAudioSource(
      uri: file.path,
      id: file.path.hashCode.toString(),
      isLocal: true,
    );
    await setSource(source);
  }

  Future<void> setSource(
    AppAudioSource source, {
    bool preload = false,
    Future<void> Function(AppAudioSource source)? onCached,
  }) async {
    log.info('setSource: ${source.uri} (isLocal: ${source.isLocal})');
    print("SoloudPlayer: setSource ${source.id} (isLocal = ${source.isLocal})");
    await stop(keepMetadata: false); // Reset everything

    // Set the callback for this session
    _onCachedCallback = onCached;

    _currentSourceController.add(source);

    if (source.isLocal) {
      if (source.uri.endsWith('.pcm')) {
        await _playPcmFile(File(source.uri));
        return;
      }
    }

    // Fetch duration async (doesn't block UI)
    unawaited(_fetchDuration(source.uri));

    if (!preload) {
      await play();
    }
  }

  Future<void> play() async {
    log.info('play() called. currentSource: ${currentSource?.uri}');
    print("SoloudPlayer: play called ${currentSource?.id}");
    if (currentSource == null) return;

    // If we are already ready/paused, just resume
    if (_processingStateController.value == ProcessingState.ready &&
        _soundHandle != null) {
      _soloud.setPause(_soundHandle!, false);
      _isPlayingController.add(true);
      return;
    }

    if (_processingStateController.value == ProcessingState.loading ||
        _processingStateController.value == ProcessingState.buffering ||
        playing) {
      return;
    }

    // Otherwise, start a fresh stream
    _seek(currentSource!, startOffset: _seekOffset);
  }

  Future<void> pause() async {
    log.info('pause() called');
    print("SoloudPlayer: pause $_soundHandle");
    if (_soundHandle != null) {
      _soloud.setPause(_soundHandle!, true);
      _isPlayingController.add(false);
    }
  }

  /// Seeking requires restarting the stream from the new offset
  Future<void> seek(Duration position) async {
    print("SoloudPlayer: seek() to $position");
    if (currentSource == null) return;

    //Temporary allow seek only when loaded completed
    if (_streamDataState != StreamDataState.fullyLoaded) return;

    // Update local state immediately for UI responsiveness
    // _seekOffset = position;
    _positionController.add(position);
    _bufferedPositionController.add(position); // Buffer resets at seek point

    // Restart stream from new offset
    _seekSubject.add(position);
  }

  Future<void> stop({bool keepMetadata = true}) async {
    log.info('stop() called');
    await _cleanup(keepMetadata: keepMetadata);
    _processingStateController.add(ProcessingState.idle);
    _isPlayingController.add(false);
    _positionController.add(Duration.zero);
    _bufferedPositionController.add(Duration.zero);
    _seekOffset = Duration.zero;
  }

  Future<void> dispose() async {
    log.info('dispose() called');
    await stop(keepMetadata: false);
    await _processingStateController.close();
    await _isPlayingController.close();
    await _positionController.close();
    await _bufferedPositionController.close();
    await _durationController.close();
    await _currentSourceController.close();
    await _seekSubject.close();
    await _errorController.close();
  }

  // --- Core Logic ---
  String? _seekOperationId;
  Future<void> _seek(
    AppAudioSource source, {
    Duration startOffset = Duration.zero,
  }) async {
    // 1. Optimize: If fully loaded and seeking forward within buffer, use native seek
    if (_streamDataState == StreamDataState.fullyLoaded &&
        _soundHandle != null &&
        startOffset >= _seekOffset &&
        !source.isLocal) {
      // Local files usually fast enough with FFmpeg, but memory buffer seek is instanant
      // Optimization applies mainly to prevent re-downloading/re-processing
      // For local files, FFmpeg restart is also fast, but native seek is better.
      // final relativePosition = startOffset - _seekOffset;
      print("SoloudPlayer: _seek() called $_soundHandle ${startOffset}");
      _soloud.seek(_soundHandle!, startOffset);
      _positionController.add(startOffset);
      return;
    }
    String localOperationId = DateTime.now().millisecondsSinceEpoch.toString();
    _seekOperationId = localOperationId;
    // 2. Set Loading State
    _processingStateController.add(ProcessingState.loading);

    // 3. Clean up previous internal pipes/sources (but keep URL/Metadata)
    await _cleanup(keepMetadata: true);

    _seekOffset = startOffset;
    _bytesBuffered = 0;
    _streamDataState = StreamDataState.buffering;
    _ffmpegReadyCompleter = Completer<void>();
    _isFfmpegFinished = false;
    _outputPipe = await FFmpegKitConfig.registerNewFFmpegPipe();

    // Setup cache sink if applicable
    IOSink? cacheSink;
    File? tempCacheFile;
    if (startOffset == Duration.zero && !source.isLocal) {
      try {
        final tempDir = await getTemporaryDirectory();
        tempCacheFile = File('${tempDir.path}/cache_${source.id}.pcm.tmp');
        cacheSink = tempCacheFile.openWrite();
      } catch (e) {
        log.warning('Failed to create cache sink: $e');
      }
    }

    // 3. Setup SoLoud Buffer
    _audioSource = _soloud.setBufferStream(
      maxBufferSizeBytes: 1024 * 1024 * 100, // 100MB
      channels: Channels.stereo,
      sampleRate: _sampleRate,
      onBuffering: (isBuffering, handle, time) {
        // Map SoLoud buffering to ProcessingState
        if (isBuffering) {
          _processingStateController.add(ProcessingState.buffering);
        } else {
          _processingStateController.add(ProcessingState.ready);
        }
      },
    );

    // 4. Start FFmpeg with Seek (-ss)
    // -ss must be BEFORE -i for fast seek
    final startSec = startOffset.inMilliseconds / 1000.0;
    final command =
        '-y -ss $startSec -i "${source.uri}" -vn -ac $_channels -ar $_sampleRate -f s16le -acodec pcm_s16le "$_outputPipe"';

    log.info('FFmpeg command: $command');

    await FFmpegKit.executeAsync(
      command,
      (session) async {
        // Session Completed
        print(
          "SoloudPlayer: play - fromStream - ffmpeg completed ${session.getReturnCode()}",
        );
        _isFfmpegFinished = true;
        if (_outputPipe != null) {
          await FFmpegKitConfig.closeFFmpegPipe(_outputPipe!);
        }
      },
      (logData) {
        log.fine('FFmpeg: ${logData.getMessage()}');
        print(
          "SoloudPlayer: play - fromStream - ffmpeg log ${logData.getMessage()}",
        );
      },
    );

    // 5. Start Pumping Data
    unawaited(
      _pumpAudioData(cacheSink: cacheSink, tempCacheFile: tempCacheFile),
    );

    // 6. Wait for initial data before playing
    // This prevents "Voice not found" errors if we play too fast
    try {
      await _ffmpegReadyCompleter?.future.timeout(const Duration(seconds: 10));
    } catch (e) {
      return;
    }

    // 7. Play
    try {
      // Check if this seek is still valid
      if (_seekOperationId != localOperationId) return;
      _soundHandle = await _soloud.play(_audioSource!);
      print("SoloudPlayer: play - fromStream ${source.id} $_soundHandle");
      _isPlayingController.add(true);
      _processingStateController.add(ProcessingState.ready);
      _startTicker();
    } catch (e) {
      log.warning('SoLoud Play Error: $e');
      print(
        "SoloudPlayer: play - fromStream - error ${e} ${source.id} $_soundHandle",
      );
      _errorController.add(e);
      _processingStateController.add(ProcessingState.idle);
    }
  }

  /// The loop that moves data from FFmpeg pipe to SoLoud buffer
  Future<void> _pumpAudioData({IOSink? cacheSink, File? tempCacheFile}) async {
    final file = File(_outputPipe!);
    RandomAccessFile? raf;

    try {
      // Retry opening pipe (sometimes OS needs a few ms)
      int retries = 0;
      while (raf == null &&
          _streamDataState == StreamDataState.buffering &&
          retries < 20) {
        try {
          print(
            "SoloudPlayer: play - fromStream - open pipe ${file.path} ${file.existsSync()}",
          );
          raf = await file.open();
        } catch (_) {
          await Future<void>.delayed(const Duration(milliseconds: 100));
          retries++;
        }
      }

      if (raf == null) return; // Failed to open pipe

      // Read Loop
      while (_streamDataState == StreamDataState.buffering) {
        // Read 16KB chunks
        final chunk = await raf.read(16384);

        if (chunk.isEmpty) {
          // If FFmpeg is finished and we got empty chunk, it's EOF.
          if (_isFfmpegFinished) {
            break;
          }
          // Otherwise, FFmpeg is thinking/downloading, wait bit
          await Future<void>.delayed(const Duration(milliseconds: 20));
          continue;
        }

        // Signal that we have at least some data
        if (_ffmpegReadyCompleter != null &&
            !_ffmpegReadyCompleter!.isCompleted) {
          _ffmpegReadyCompleter!.complete();
        }

        if (_audioSource != null) {
          _soloud.addAudioDataStream(_audioSource!, chunk);

          // Write to cache if active
          cacheSink?.add(chunk);

          // Track buffered bytes for "Gray Bar"
          _bytesBuffered += chunk.length;
          final bufferedDuration = Duration(
            milliseconds: ((_bytesBuffered / _bytesPerSecond) * 1000).toInt(),
          );
          // The buffer position is StartOffset + BytesRead
          _bufferedPositionController.add(_seekOffset + bufferedDuration);
        }
      }

      // If we exited the loop normally (and didn't error/cleanup), we are fully loaded
      if (_streamDataState == StreamDataState.buffering) {
        _streamDataState = StreamDataState.fullyLoaded;

        // Finalize Cache
        if (cacheSink != null && tempCacheFile != null) {
          await cacheSink.flush();
          await cacheSink.close();
          cacheSink = null; // Prevent double close in finally

          // Rename .tmp to .pcm
          final pcmPath = tempCacheFile.path.replaceAll('.tmp', '');
          final pcmFile = await tempCacheFile.rename(pcmPath);
          log.info('PCM Cached: $pcmPath');

          if (_onCachedCallback != null) {
            final cachedSource = AppAudioSource(
              uri: pcmFile.path,
              id: currentSource?.id ?? 'unknown',
              isLocal: true,
            );
            await _onCachedCallback!(cachedSource);
            // Auto-delete after callback returns
            if (pcmFile.existsSync()) {
              await pcmFile.delete();
              log.info('PCM Cache deleted after callback');
            }
          }
        }
      }
    } catch (e) {
      log.warning('Pump Error: $e');
      await raf?.close();
      await cacheSink?.close();
    }
  }

  /// Updates Position and State (Completed check)
  void _startTicker() {
    _stateTicker?.cancel();
    _stateTicker = Timer.periodic(const Duration(milliseconds: 200), (_) {
      // log.finest('Updating position');
      // log.finest('Sound handle: ${_soundHandle}');
      if (_soundHandle == null) return;

      if (_soloud.getIsValidVoiceHandle(_soundHandle!)) {
        // 1. Update Playback Position
        final currentPos = _soloud.getPosition(_soundHandle!);
        print(
          "SoloudPlayer: play - fromStream - currentPos  $_seekOffset + $currentPos = ${_seekOffset + currentPos}",
        );
        _positionController.add(_seekOffset + currentPos);

        // 2. Sync Play/Pause state from engine (optional safety)
        final isPaused = _soloud.getPause(_soundHandle!);
        if (isPaused != !_isPlayingController.value) {
          _isPlayingController.add(!isPaused);
        }
      } else {
        // Handle Invalid -> Playback Stopped
        // If FFmpeg is also done, we are truly completed.
        if (_streamDataState != StreamDataState.buffering) {
          _processingStateController.add(ProcessingState.completed);
          _isPlayingController.add(false);
          _stateTicker?.cancel();
        }
      }
    });
  }

  Future<void> _fetchDuration(String url) async {
    // Reset duration
    _durationController.add(Duration.zero);

    await FFprobeKit.execute(
      '-v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$url"',
    ).then((session) async {
      final output = await session.getOutput();
      if (output != null) {
        final sec = double.tryParse(output.trim()) ?? 0;
        _durationController.add(Duration(milliseconds: (sec * 1000).toInt()));
      }
    });
  }

  Future<void> _cleanup({bool keepMetadata = false}) async {
    _streamDataState = StreamDataState.idle; // Stop the pump loop
    _stateTicker?.cancel();

    if (_outputPipe != null) {
      await FFmpegKitConfig.closeFFmpegPipe(_outputPipe!);
    }

    if (_soundHandle != null) {
      // Trying to stop an invalid handle might throw, so we ignore
      try {
        print("SoloudPlayer: stop $_soundHandle");
        await _soloud.stop(_soundHandle!);
      } catch (_) {}
      _soundHandle = null;
    }

    if (_audioSource != null) {
      try {
        await _soloud.disposeAllSources();
      } catch (_) {}
      _audioSource = null;
    }

    if (!keepMetadata) {
      _currentSourceController.add(null);
      _seekOffset = Duration.zero;
    }
  }

  //data/user/0/com.smartdevice.speaker/files/cached_tracks/cache_aAkMkVFwAoo.pcm: Invalid data found when processing input
  Future<void> _playPcmFile(File file) async {
    await stop(keepMetadata: true);
    _isPlayingController.add(true);
    _streamDataState =
        StreamDataState.fullyLoaded; // It's local, so instant load
    _processingStateController.add(ProcessingState.loading);

    try {
      _audioSource = _soloud.setBufferStream(
        maxBufferSizeBytes: 1024 * 1024 * 100, // 100MB
        channels: Channels.stereo,
        sampleRate: _sampleRate,
        onBuffering: (_, __, ___) {},
      );
      _processingStateController.add(ProcessingState.ready);
      print("SoloudPlayer: play - fromPCM ${file.uri} $_soundHandle");
      _soundHandle = await _soloud.play(_audioSource!);
      print("SoloudPlayer: play $_soundHandle");
      _startTicker();

      // Read file and push chunks
      // PCM file is just raw bytes
      final raf = await file.open();
      // Read entire file or chunks? For 30MB file, chunks is safer for UI jank
      // But we can read large chunks.
      const int bufferSize = 64 * 1024;
      while (true) {
        if (_soundHandle == null) break; // Stopped
        final chunk = await raf.read(bufferSize);
        if (chunk.isEmpty) break;
        _soloud.addAudioDataStream(_audioSource!, chunk);
        // Small yield to let UI breathe
        await Future<void>.delayed(Duration.zero);
      }
      await raf.close();

      // Calculate duration from file size
      final len = await file.length();
      final duration = Duration(
        milliseconds: ((len / _bytesPerSecond) * 1000).toInt(),
      );
      _durationController.add(duration);
    } catch (e) {
      log.severe('Error playing PCM file: $e');
      _errorController.add(e);
      await stop();
    }
  }
}
