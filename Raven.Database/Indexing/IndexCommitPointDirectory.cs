﻿// -----------------------------------------------------------------------
//  <copyright file="IndexCommitPointDirectory.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Globalization;
using System.IO;
using Raven.Database.Extensions;

namespace Raven.Database.Indexing
{
	public class IndexCommitPointDirectory
	{
		private const string DirectoryName = "CommitPoints";
		private const string File = "index.commitPoint";

		public IndexCommitPointDirectory(string indexStoragePath, string indexName, string name)
		{
			IndexFullPath = Path.Combine(indexStoragePath, MonoHttpUtility.UrlEncode(indexName));
			AllCommitPointsFullPath = GetAllCommitPointsFullPath(IndexFullPath);
			Name = name;
			FullPath = Path.Combine(AllCommitPointsFullPath, Name);
			FileFullPath = Path.Combine(FullPath, File);
		}

		public string IndexFullPath { get; private set; }

		public string AllCommitPointsFullPath { get; private set; }

		public string Name { get; private set; }

		public string FullPath { get; private set; }

		public string FileFullPath { get; private set; }

		public static string GetAllCommitPointsFullPath(string indexFullPath)
		{
			return Path.Combine(indexFullPath, DirectoryName);
		}

		public static string[] ScanAllCommitPointsDirectory(string indexFullPath)
		{
			return Directory.GetDirectories(Path.Combine(indexFullPath, DirectoryName));
		}
	}
}