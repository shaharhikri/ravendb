﻿using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Client.Json
{
    internal class BlittableJsonContent : HttpContent
    {
        private readonly Func<Stream, Task> _asyncTaskWriter;

        public BlittableJsonContent(Func<Stream, Task> writer)
        {
            _asyncTaskWriter = writer ?? throw new ArgumentNullException(nameof(writer));
            Headers.ContentEncoding.Add("gzip");
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            // Immediately flush request stream to send headers
            // https://github.com/dotnet/corefx/issues/39586#issuecomment-516210081
            // https://github.com/dotnet/runtime/issues/96223#issuecomment-1865009861
            await stream.FlushAsync().ConfigureAwait(false);

#if NETSTANDARD2_0 || NETCOREAPP2_1
            using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
#else
            await using (var gzipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
#endif
            {
                await _asyncTaskWriter(gzipStream).ConfigureAwait(false);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;
            return false;
        }
    }
}
