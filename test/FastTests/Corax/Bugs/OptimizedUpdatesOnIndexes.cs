﻿using System;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public unsafe class OptimizedUpdatesOnIndexes : StorageTest
{
    public OptimizedUpdatesOnIndexes(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanUpdateAndNotDelete()
    {
        using var fields = CreateKnownFields(Allocator);
        long oldId; 
        using (var indexWriter = new IndexWriter(Env, fields))
        {
            using (var entry = indexWriter.Update("cars/1"u8))
            {
                entry.Write(0, "cars/1"u8);
                entry.Write(1, "Lightning"u8);
                entry.Write(2, "12"u8);
                oldId = entry.EntryId;
            }
            
            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, fields))
        {
            var termQuery = indexSearcher.TermQuery("Age", "12");
            var ids = new long[16];
            var read = termQuery.Fill(ids);
            Assert.Equal(1, read);
            Assert.Equal(oldId, ids[0]);
        }

        long newId;
        using (var indexWriter = new IndexWriter(Env, fields))
        {
            using (var entry = indexWriter.Update("cars/1"u8))
            {
                entry.Write(0, "cars/1"u8);
                entry.Write(1, "Lightning"u8);
                entry.Write(2, "13"u8);
                newId = entry.EntryId;
            }
            
            indexWriter.Commit();
        }
        Assert.Equal(oldId, newId);
        
        using (var indexSearcher = new IndexSearcher(Env, fields))
        {
            var termQuery = indexSearcher.TermQuery("Age", "12");
            var ids = new long[16];
            var read = termQuery.Fill(ids);
            Assert.Equal(0, read);
            termQuery = indexSearcher.TermQuery("Age", "13");
            ids = new long[16];
            read = termQuery.Fill(ids);
            Assert.Equal(1, read);
            Assert.Equal(oldId, ids[0]);
        }

    }
    
    private IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "id()", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice nameSlice);
        Slice.From(ctx, "Age", ByteStringType.Immutable, out Slice ageSlice);

        var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, idSlice)
            .AddBinding(1, nameSlice)
            .AddBinding(2, ageSlice);
        return builder.Build();
    }

}
