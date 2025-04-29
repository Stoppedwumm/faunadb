package fauna.net.test

import java.nio.file._
import fauna.net.security._
import java.security.cert.X509Certificate

class PEMFileSpec extends Spec {

  val pw = Password("secret")
  val selfSigned = "CN=FaunaDB Adhoc Self-Signed"

  def certCN(cert: X509Certificate) = cert.getSubjectX500Principal.getName

  "PEMFile" - {
    "enumerates contained objects" in {
      testPEMFile("private_key1.pem").objects.size should equal(1)
      testPEMFile("private_key1.pem").decryptedObjects(pw).size should equal(1)
      testPEMFile("private_key1.pem").privateKeys(pw).size should equal(1)
      testPEMFile("private_key1.pem").trustedKeys(pw).size should equal(1)
      testPEMFile("private_key1.pem").trustedCerts.size should equal(0)
      testPEMFile("private_key1.pem").trustedCRLs.size should equal(0)

      testPEMFile("encrypted_private_key1.pem").objects.size should equal(1)
      testPEMFile("encrypted_private_key1.pem").decryptedObjects(pw).size should equal(1)
      testPEMFile("encrypted_private_key1.pem").privateKeys(pw).size should equal(1)
      testPEMFile("encrypted_private_key1.pem").trustedKeys(pw).size should equal(1)
      testPEMFile("encrypted_private_key1.pem").trustedCerts.size should equal(0)
      testPEMFile("encrypted_private_key1.pem").trustedCRLs.size should equal(0)

      testPEMFile("public_key1.pem").objects.size should equal(1)
      testPEMFile("public_key1.pem").decryptedObjects(pw).size should equal(1)
      testPEMFile("public_key1.pem").privateKeys(pw).size should equal(0)
      testPEMFile("public_key1.pem").trustedKeys(pw).size should equal(1)
      testPEMFile("public_key1.pem").trustedCerts.size should equal(0)
      testPEMFile("public_key1.pem").trustedCRLs.size should equal(0)

      testPEMFile("all_public_keys.pem").objects.size should equal(2)
      testPEMFile("all_public_keys.pem").decryptedObjects(pw).size should equal(2)
      testPEMFile("all_public_keys.pem").privateKeys(pw).size should equal(0)
      testPEMFile("all_public_keys.pem").trustedKeys(pw).size should equal(2)
      testPEMFile("all_public_keys.pem").trustedCerts.size should equal(0)
      testPEMFile("all_public_keys.pem").trustedCRLs.size should equal(0)

      testPEMFile("root_trust.pem").objects.size should equal(3)
      testPEMFile("root_trust.pem").decryptedObjects(pw).size should equal(3)
      testPEMFile("root_trust.pem").privateKeys(pw).size should equal(0)
      testPEMFile("root_trust.pem").trustedKeys(pw).size should equal(2)
      testPEMFile("root_trust.pem").trustedCerts.size should equal(2)
      testPEMFile("root_trust.pem").trustedCRLs.size should equal(1)

      testPEMFile("encrypted_server_cert_chain1.pem").objects.size should equal(4)
      testPEMFile("encrypted_server_cert_chain1.pem").decryptedObjects(pw).size should equal(4)
      testPEMFile("encrypted_server_cert_chain1.pem").privateKeys(pw).size should equal(1)
      testPEMFile("encrypted_server_cert_chain1.pem").trustedKeys(pw).size should equal(4)
      testPEMFile("encrypted_server_cert_chain1.pem").trustedCerts.size should equal(3)
      testPEMFile("encrypted_server_cert_chain1.pem").trustedCRLs.size should equal(0)
    }

    "write" - {
      "writes an encryted key" in {
        val src = testPEMFile("private_key1.pem")
        val List(key) = src.privateKeys(Password.empty)

        val tmp = Files.createTempFile(null, null)
        val newpw = Password("foo")

        PEMFile.write(tmp, newpw, List(key))
        PEMFile(tmp).privateKeys(newpw) should equal (List(key))
      }

      "writes a cert chain" in {
        val src = testPEMFile("encrypted_server_cert_chain1.pem")
        val (key, chain) = src.keyedCert(pw).get

        val tmp = Files.createTempFile(null, null)
        val newpw = Password("foo")

        PEMFile.write(tmp, newpw, chain :+ key)
        PEMFile(tmp).keyedCert(newpw) should equal (Some((key, chain)))
      }
    }

    "decryptedObjects" - {
      "enumerates objects if pw is correct or no pw is required" in {
        testPEMFile("private_key1.pem").decryptedObjects(Password.empty).size should equal(1)
        testPEMFile("encrypted_private_key1.pem").decryptedObjects(pw).size should equal(1)
      }

      "fails if pw is incorrect or has been destroyed" in {
        val pem = testPEMFile("encrypted_private_key1.pem")
        val pw = Password("secret")

        noException should be thrownBy pem.decryptedObjects(pw)
        pw.destroy()

        a[DestroyedPasswordException] should be thrownBy pem.decryptedObjects(pw)
        a[NoPasswordException] should be thrownBy pem.decryptedObjects(Password.empty)
        a[IncorrectPasswordException] should be thrownBy pem.decryptedObjects(Password("foo"))
      }
    }

    "keyedCert" - {
      "returns an adhoc cert public/private key pair is present with no certs" in {
        val Some((_, Seq(cert1))) = testPEMFile("private_key1.pem").keyedCert(pw)
        certCN(cert1) should equal (selfSigned)

        val Some((_, Seq(cert2))) = testPEMFile("encrypted_private_key1.pem").keyedCert(pw)
        certCN(cert2) should equal (selfSigned)
      }

      "returns None if no private key is present" in {
        testPEMFile("public_key1.pem").keyedCert(pw) should equal (None)
        testPEMFile("all_public_keys.pem").keyedCert(pw) should equal (None)
      }

      "returns a cert chain for combined pem" in {
        val Some((_, Seq(cert, int, root))) = testPEMFile("encrypted_server_cert_chain1.pem").keyedCert(pw)
        certCN(cert) should equal ("CN=localhost,OU=Test Server 1,O=FaunaDB,ST=CA,C=US")
        certCN(int) should equal ("CN=Test Intermediate CA,O=FaunaDB,ST=CA,C=US")
        certCN(root) should equal ("CN=Test Root CA,O=FaunaDB,ST=CA,C=US")
      }
    }

    "keyManagerFactory" - {
      "returns a KeyManagerFactory if private key and a public key or cert is present" in {
        testPEMFile("private_key1.pem").keyManagerFactory(pw).isDefined should be(true)
        testPEMFile("encrypted_private_key1.pem").keyManagerFactory(pw).isDefined should be(true)
        testPEMFile("encrypted_server_cert_chain1.pem").keyManagerFactory(pw).isDefined should be(true)
      }

      "returns None if a KeyManagerFactory cannot be provided" in {
        val pubKey = testPEMFile("public_key1.pem")
        val allKeys = testPEMFile("all_public_keys.pem")

        pubKey.keyManagerFactory(pw) should equal (None)
        allKeys.keyManagerFactory(pw) should equal (None)
      }
    }

    "trustManagerFactory" - {
      "returns a TrustManagerFactory if at least one public key or cert is present" in {
        testPEMFile("private_key1.pem").trustManagerFactory(pw).isDefined should be(true)
        testPEMFile("public_key1.pem").trustManagerFactory(pw).isDefined should be(true)
        testPEMFile("encrypted_private_key1.pem").trustManagerFactory(pw).isDefined should be(true)
        testPEMFile("encrypted_server_cert_chain1.pem").trustManagerFactory(pw).isDefined should be(true)
        testPEMFile("all_public_keys.pem").trustManagerFactory(pw).isDefined should be(true)
        testPEMFile("root_trust.pem").trustManagerFactory(pw).isDefined should be(true)
      }

      "returns None if a TrustManagerFactory cannot be provided" in {
        testPEMFile("empty.pem").trustManagerFactory(pw) should equal (None)
      }
    }
  }
}
