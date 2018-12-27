import java.io.InputStream
import java.util.Properties

object PropertiesUtils{
  def getProperties(path:String):Properties = {
    val properties = new Properties()
    val in: InputStream = PropertiesUtils.getClass.getClassLoader.getResourceAsStream(path)
    properties.load(in)
    properties
  }

  def loadProperties(key:String):String = {
    val properties = new Properties()
    val in: InputStream = PropertiesUtils.getClass.getClassLoader.getResourceAsStream("config.properties")
    properties.load(in)
    properties.getProperty(key)
  }
}