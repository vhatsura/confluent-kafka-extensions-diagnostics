using System.Diagnostics.CodeAnalysis;
using Xunit.Extensions.AssemblyFixture;

[assembly: ExcludeFromCodeCoverage]
[assembly: TestFramework(AssemblyFixtureFramework.TypeName, AssemblyFixtureFramework.AssemblyName)]
