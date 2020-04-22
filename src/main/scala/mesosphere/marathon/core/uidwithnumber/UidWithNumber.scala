package mesosphere.marathon
package core.uidwithnumber

import java.util.UUID

import com.fasterxml.uuid.{EthernetAddress, Generators}

class UidWithNumber {
  private val uuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface())

  def generate(number: Int): UUID = {
    var uid = uuidGenerator.generate()
    if (number > 0) {
      // put version to 0 (instead of 2)
      // n.b. doesn't work with negative numbers
      new UUID(uid.getMostSignificantBits(), (uid.getLeastSignificantBits() & 0x3FFFFFFF00000000L) | number)
    } else {
      uid
    }
  }
}
