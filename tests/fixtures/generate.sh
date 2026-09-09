#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

if [[ $# -ne 1 || -z $1 ]]; then
    printf 'Usage: bash %s NEW_OUTPUT_DIRECTORY\n' "$0" >&2
    exit 2
fi

output=$1
parent=$(dirname -- "$output")
if [[ ! -d $parent || -e $output || -L $output ]]; then
    printf 'Output must be a new directory beneath an existing parent: %s\n' "$output" >&2
    exit 2
fi

# An absolute path also prevents ffmpeg interpreting a directory name as a URL.
output="$(realpath -- "$parent")/$(basename -- "$output")"

command -v ffmpeg >/dev/null
command -v nix >/dev/null

# Resolve the font from the same locked packages as the development shell.
repository=$(realpath -- "$(dirname -- "${BASH_SOURCE[0]}")/../..")
export FIXTURE_REPOSITORY="$repository"
font_package=$(nix build --no-link --print-out-paths --impure --expr \
    '(builtins.getFlake ("git+file://" + builtins.getEnv "FIXTURE_REPOSITORY")).inputs.nixpkgs.legacyPackages.${builtins.currentSystem}.dejavu_fonts')
font="$font_package/share/fonts/truetype/DejaVuSansMono.ttf"
test -r "$font"

# mkdir is exclusive; ffmpeg -n also refuses to replace any individual file.
mkdir -- "$output"
ffmpeg_options=(-hide_banner -loglevel error -nostdin -n -filter_threads 1)
text="drawtext=fontfile=$font:fontcolor=white:box=1:boxcolor=black:boxborderw=6"
grid="color=c=black:s=960x640:r=1,drawbox=x=0:y=0:w=480:h=320:color=red:t=fill,drawbox=x=480:y=0:w=480:h=320:color=green:t=fill,drawbox=x=0:y=320:w=480:h=320:color=blue:t=fill,drawbox=x=480:y=320:w=480:h=320:color=yellow:t=fill"
corners="$text:text='TOP LEFT RED':fontsize=28:x=24:y=90,$text:text='TOP RIGHT GREEN':fontsize=28:x=504:y=90,$text:text='BOTTOM LEFT BLUE':fontsize=28:x=24:y=540,$text:text='BOTTOM RIGHT YELLOW':fontsize=28:x=504:y=540"

for rendition in original original-preview generated-display generated-preview; do
    case "$rendition" in
        original) label='ORIGINAL JPEG'; size=960x640 ;;
        original-preview) label='ORIGINAL PREVIEW'; size=480x320 ;;
        generated-display) label='GENERATED DISPLAY'; size=960x640 ;;
        generated-preview) label='GENERATED PREVIEW'; size=480x320 ;;
    esac

    ffmpeg "${ffmpeg_options[@]}" -f lavfi -i "$grid" \
        -vf "$corners,$text:text='$label':fontsize=42:x=(w-tw)/2:y=(h-th)/2,scale=$size" \
        -frames:v 1 -c:v mjpeg -q:v 2 -pix_fmt yuvj420p -threads 1 \
        -map_metadata -1 -fflags +bitexact -flags:v +bitexact \
        -update 1 "$output/$rendition.jpg"
done

for rendition in original playback; do
    case "$rendition" in
        original) label='ORIGINAL 640x360'; size=640x360; frequency=440 ;;
        playback) label='PLAYBACK 480x270'; size=480x270; frequency=880 ;;
    esac

    # Independent labels and tones make selecting the second resource observable.
    ffmpeg "${ffmpeg_options[@]}" \
        -f lavfi -i "testsrc2=size=$size:rate=30:duration=30" \
        -f lavfi -i "sine=frequency=$frequency:sample_rate=48000:duration=30" \
        -map 0:v:0 -map 1:a:0 \
        -vf "$text:text='$label':fontsize=24:x=16:y=16,$text:text='FRAME %{n}':fontsize=24:x=16:y=h-48" \
        -c:v libx264 -preset medium -crf 23 -profile:v baseline -level:v 3.0 \
        -pix_fmt yuv420p -threads:v 1 -g 30 -keyint_min 30 -sc_threshold 0 \
        -c:a aac -b:a 128k -ac 2 -threads:a 1 -t 30 \
        -map_metadata -1 -fflags +bitexact -flags:v +bitexact -flags:a +bitexact \
        -movflags +faststart "$output/video-$rendition.mp4"
done

printf 'Generated six fixture files in %s\n' "$output"
