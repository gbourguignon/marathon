package mesosphere.marathon
package tasks

import collection.JavaConverters._
import org.apache.mesos.Protos.Labels

object TaskLabelGetter {

  def getLabels(mesosLabels: Labels): Map[String, String] = {
    if (mesosLabels == null)
      return Map.empty[String, String]
    val labelList = mesosLabels.getLabelsList
    if (labelList.isEmpty)
      Map.empty[String, String]
    else
      labelList.asScala.map(label => (label.getKey, if (label.hasValue) label.getValue else "")).toMap
  }
}
