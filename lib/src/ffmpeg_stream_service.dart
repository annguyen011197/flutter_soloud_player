import 'dart:async';
import 'dart:io';

import 'package:ffmpeg_kit_flutter_new_https/ffmpeg_kit.dart';
import 'package:ffmpeg_kit_flutter_new_https/ffmpeg_kit_config.dart';
import 'package:ffmpeg_kit_flutter_new_https/session.dart';
import 'package:logging/logging.dart';

/// Service to handle streaming audio via FFMPEG using the Tee Muxer.
///
/// This service allows simultaneous:
/// 1. Streaming of raw PCM data to a Named Pipe (for immediate playback).
/// 2. Caching of the audio as a valid .wav file to disk.
class FFmpegStreamService {
  final _log = Logger('FFmpegStreamService');

  /// Starts the FFMPEG stream.
  ///
  /// [url] is the input audio URL.
  /// [cachePath] is the absolute path where the .wav file should be saved.
  ///
  /// Returns a [StreamSession] containing the pipe path and the FFmpeg session.
  Future<StreamSession> streamAndCache(String url, String cachePath) async {
    // 1. Create a Named Pipe to stream raw PCM data to the player
    final pipePath = await FFmpegKitConfig.registerNewFFmpegPipe();
    if (pipePath == null) {
      throw Exception('Failed to create FFmpeg pipe');
    }
    _log.info('Created pipe: $pipePath');

    // 2. Construct the FFMPEG command using the Tee Muxer
    //
    // -y: Overwrite output files without asking
    // -i "$url": Input URL
    // -map 0:a: Select the audio stream from the input
    // -ac 2: Stereo channels
    // -ar 44100: 44.1kHz sample rate
    // -c:a pcm_s16le: Convert audio to 16-bit PCM
    // -f tee: Use the Tee Muxer
    //
    // The tee muxer string: "[f=s16le]$pipePath|[f=wav]$cachePath"
    // - [f=s16le]$pipePath: Write raw PCM s16le data to the named pipe
    // - [f=wav]$cachePath: Write a valid WAV file to the cache path
    //
    // Note: We use double quotes around the tee argument to ensure paths with spaces are handled,
    // though typically the tee parser handles them if formatted correctly.

    // Ensure cache directory exists
    final cacheFile = File(cachePath);
    if (!cacheFile.parent.existsSync()) {
      cacheFile.parent.createSync(recursive: true);
    }

    // Escape paths for FFMPEG command line if necessary (basic quote wrapping is usually enough)
    // For Tee muxer, the separator is '|'. If paths contain '|', it gets complex, but standard paths won't.
    final teeOutput = "[f=s16le]$pipePath|[f=wav]$cachePath";

    // Command Breakdown:
    // -loglevel error: Reduce noise
    // -re: OMITTED. We want to download/transcode as fast as network allows for buffering purposes.
    final command =
        '-y -i "$url" -map 0:a -ac 2 -ar 44100 -c:a pcm_s16le -f tee "$teeOutput"';

    _log.info('Executing FFmpeg command: $command');

    // 3. Execute Async
    // We don't await the completion here because it's a stream.
    // We return the session so the caller can manage it.
    final session = await FFmpegKit.executeAsync(
      command,
      (session) async {
        final returnCode = await session.getReturnCode();
        if (returnCode != null && returnCode.isValueSuccess()) {
          _log.info('FFmpeg process completed successfully.');
        } else {
          _log.warning('FFmpeg process failed with rc=$returnCode');
        }
        // Always close the pipe when session ends
        await FFmpegKitConfig.closeFFmpegPipe(pipePath);
      },
      (log) {
        // Optional: Filter logs
        // final message = log.getMessage();
        // if (message != null) _log.fine(message);
      },
      (statistics) {
        // Optional: Update progress
      },
    );

    return StreamSession(pipePath: pipePath, session: session);
  }
}

/// A simple wrapper to hold the session and the pipe path.
class StreamSession {
  final String pipePath;
  final Session session;

  StreamSession({required this.pipePath, required this.session});

  /// Cancels the FFmpeg session and cleans up the pipe.
  Future<void> cancel() async {
    await session.cancel();
    await FFmpegKitConfig.closeFFmpegPipe(pipePath);
  }
}

/*
/// Usage Snippet for SoloudAudioPlayer:
///
/// ```dart
/// final ffmpegService = FFmpegStreamService();
///
/// // Start the stream
/// final cacheDir = await getTemporaryDirectory();
/// final cachePath = '${cacheDir.path}/track_${id}.wav';
/// final streamSession = await ffmpegService.streamAndCache(url, cachePath);
///
/// // Consume the PCM data from the pipe
/// final pipeFile = File(streamSession.pipePath);
/// 
/// // Wait for pipe to be ready (FFmpeg takes a moment to open it)
/// RandomAccessFile? raf;
/// int retries = 0;
/// while (raf == null && retries < 20) {
///   try {
///     raf = await pipeFile.open();
///   } catch (_) {
///     await Future.delayed(const Duration(milliseconds: 100));
///     retries++;
///   }
/// }
///
/// if (raf != null) {
///   // Read loop
///   while (active) {
///      final chunk = await raf.read(16384);
///      if (chunk.isEmpty) {
///         // Check if session finished
///         if (await streamSession.session.getState() == SessionState.completed) break;
///         await Future.delayed(const Duration(milliseconds: 20));
///         continue;
///      }
///      // Feed to player
///      soloud.addAudioDataStream(source, chunk);
///   }
///   await raf.close();
/// }
/// ```
*/
