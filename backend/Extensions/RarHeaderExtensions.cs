using System.Buffers;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using NzbWebDAV.Models;
using SharpCompress.Common.Rar.Headers;

namespace NzbWebDAV.Extensions;

/// <summary>
/// High-performance RAR header extension methods with optimized AES key derivation.
/// </summary>
public static class RarHeaderExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte GetCompressionMethod(this IRarHeader header)
    {
        return (byte)header.GetReflectionProperty("CompressionMethod")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetDataStartPosition(this IRarHeader header)
    {
        return (long)header.GetReflectionProperty("DataStartPosition")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetAdditionalDataSize(this IRarHeader header)
    {
        return (long)header.GetReflectionProperty("AdditionalDataSize")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetCompressedSize(this IRarHeader header)
    {
        return (long)header.GetReflectionProperty("CompressedSize")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long GetUncompressedSize(this IRarHeader header)
    {
        return (long)header.GetReflectionProperty("UncompressedSize")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static string GetFileName(this IRarHeader header)
    {
        return (string)header.GetReflectionProperty("FileName")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsDirectory(this IRarHeader header)
    {
        return (bool)header.GetReflectionProperty("IsDirectory")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int? GetVolumeNumber(this IRarHeader header)
    {
        return header.HeaderType == HeaderType.Archive
            ? (int?)header.GetReflectionProperty("VolumeNumber")
            : (short?)header.GetReflectionProperty("VolumeNumber");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool GetIsFirstVolume(this IRarHeader header)
    {
        return (bool)header.GetReflectionProperty("IsFirstVolume")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static byte[]? GetR4Salt(this IRarHeader header)
    {
        return (byte[]?)header.GetReflectionProperty("R4Salt")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object? GetRar5CryptoInfo(this IRarHeader header)
    {
        return header.GetReflectionProperty("Rar5CryptoInfo")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool GetIsEncrypted(this IRarHeader header)
    {
        return (bool)header.GetReflectionProperty("IsEncrypted")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool GetIsSolid(this IRarHeader header)
    {
        return (bool)header.GetReflectionProperty("IsSolid")!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static AesParams? GetAesParams(this IRarHeader header, string? password)
    {
        // sanity checks
        if (header.HeaderType != HeaderType.File) return null;
        if (password == null) return null;

        // rar3 aes params
        var r4Salt = header.GetR4Salt();
        if (r4Salt != null) return GetRar3AesParams(r4Salt, password, header.GetUncompressedSize());

        // rar5 aes params
        var rar5CryptoInfo = header.GetRar5CryptoInfo();
        if (rar5CryptoInfo != null) return GetRar5AesParams(rar5CryptoInfo, password, header.GetUncompressedSize());

        // no aes params
        return null;
    }

    /// <summary>
    /// RAR3 AES key derivation using ArrayPool to avoid large heap allocations.
    /// Note: This algorithm is intentionally slow for security (262K iterations).
    /// </summary>
    private static AesParams GetRar3AesParams(byte[] salt, string password, long decodedSize)
    {
        const int sizeInitV = 0x10;
        const int sizeSalt30 = 0x08;
        const int noOfRounds = 1 << 18; // 262144
        const int iblock = 3;

        var aesIV = new byte[sizeInitV];

        var rawLength = 2 * password.Length;
        var blockSize = rawLength + sizeSalt30;
        var dataBlockSize = blockSize + iblock;
        var totalDataSize = dataBlockSize * noOfRounds;

        // Use ArrayPool to avoid LOH allocation for the large buffer
        var data = ArrayPool<byte>.Shared.Rent(totalDataSize);
        try
        {
            // Build raw password template
            var passwordBytes = Encoding.UTF8.GetBytes(password);

            // Fill each block with password + salt + counter
            for (var i = 0; i < noOfRounds; i++)
            {
                var offset = i * dataBlockSize;

                // Copy password in UTF-16LE format
                for (var p = 0; p < password.Length; p++)
                {
                    data[offset + p * 2] = passwordBytes[p];
                    data[offset + p * 2 + 1] = 0;
                }

                // Copy salt
                Buffer.BlockCopy(salt, 0, data, offset + rawLength, Math.Min(salt.Length, sizeSalt30));

                // Set round counter bytes (little-endian 24-bit)
                data[offset + blockSize] = (byte)i;
                data[offset + blockSize + 1] = (byte)(i >> 8);
                data[offset + blockSize + 2] = (byte)(i >> 16);
            }

            // Compute hashes using SHA1
            using var sha1 = SHA1.Create();
            var ivInterval = noOfRounds / sizeInitV;

            // Extract IV bytes at intervals
            for (var i = 0; i < sizeInitV; i++)
            {
                var roundIndex = i * ivInterval;
                var hashLength = (roundIndex + 1) * dataBlockSize;
                var digest = sha1.ComputeHash(data, 0, hashLength);
                aesIV[i] = digest[19];
            }

            // Compute final hash
            var finalDigest = sha1.ComputeHash(data, 0, totalDataSize);

            // Build AES key from digest (byte reordering)
            var aesKey = new byte[sizeInitV];
            for (var i = 0; i < 4; i++)
            {
                var word = ((uint)finalDigest[i * 4] << 24) |
                          ((uint)finalDigest[i * 4 + 1] << 16) |
                          ((uint)finalDigest[i * 4 + 2] << 8) |
                          finalDigest[i * 4 + 3];
                aesKey[i * 4] = (byte)word;
                aesKey[i * 4 + 1] = (byte)(word >> 8);
                aesKey[i * 4 + 2] = (byte)(word >> 16);
                aesKey[i * 4 + 3] = (byte)(word >> 24);
            }

            return new AesParams
            {
                Iv = aesIV,
                Key = aesKey,
                DecodedSize = decodedSize,
            };
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(data, clearArray: true);
        }
    }

    private static AesParams GetRar5AesParams(object rar5CryptoInfo, string password, long decodedSize)
    {
        const int sizePswCheck = 0x08;
        const int sha256DigestSize = 32;

        var lg2Count = (int)rar5CryptoInfo.GetReflectionField("LG2Count")!;
        var salt = (byte[])rar5CryptoInfo.GetReflectionField("Salt")!;
        var usePswCheck = (bool)rar5CryptoInfo.GetReflectionField("UsePswCheck")!;
        var pswCheck = (byte[])rar5CryptoInfo.GetReflectionField("PswCheck")!;
        var initIv = (byte[])rar5CryptoInfo.GetReflectionField("InitV")!;

        var iterations = 1 << lg2Count;

        // Build salt with counter suffix
        var saltWithCounter = new byte[salt.Length + 4];
        Buffer.BlockCopy(salt, 0, saltWithCounter, 0, salt.Length);
        saltWithCounter[salt.Length + 3] = 1; // Counter = 1 (big-endian)

        var derivedKey = GenerateRarPbkdf2Key(password, saltWithCounter, iterations);

        // Verify password check
        Span<byte> derivedPswCheck = stackalloc byte[sizePswCheck];
        derivedPswCheck.Clear();

        for (var i = 0; i < sha256DigestSize; i++)
        {
            derivedPswCheck[i % sizePswCheck] ^= derivedKey[2][i];
        }

        if (usePswCheck && !pswCheck.AsSpan().SequenceEqual(derivedPswCheck))
        {
            throw new CryptographicException("The password did not match.");
        }

        return new AesParams
        {
            Iv = initIv,
            Key = derivedKey[0],
            DecodedSize = decodedSize,
        };
    }

    /// <summary>
    /// Optimized PBKDF2 key generation for RAR5 with reduced allocations.
    /// </summary>
    private static List<byte[]> GenerateRarPbkdf2Key(string password, byte[] salt, int iterations)
    {
        var passwordBytes = Encoding.UTF8.GetBytes(password);
        using var hmac = new HMACSHA256(passwordBytes);

        var block = hmac.ComputeHash(salt);
        var finalHash = new byte[block.Length];
        Buffer.BlockCopy(block, 0, finalHash, 0, block.Length);

        ReadOnlySpan<int> loops = [iterations, 17, 17];
        var result = new List<byte[]>(3);

        for (var x = 0; x < 3; x++)
        {
            var loopCount = loops[x];
            for (var i = 1; i < loopCount; i++)
            {
                block = hmac.ComputeHash(block);
                // XOR in-place
                for (var j = 0; j < finalHash.Length; j++)
                {
                    finalHash[j] ^= block[j];
                }
            }

            var hashCopy = new byte[finalHash.Length];
            Buffer.BlockCopy(finalHash, 0, hashCopy, 0, finalHash.Length);
            result.Add(hashCopy);
        }

        return result;
    }
}
