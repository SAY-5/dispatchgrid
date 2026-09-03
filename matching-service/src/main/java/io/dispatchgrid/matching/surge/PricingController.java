package io.dispatchgrid.matching.surge;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Read side of the surge signal: the whole city grid, or a quote for one pickup point. */
@RestController
@RequestMapping("/pricing")
public class PricingController {
  private final SurgeTracker surge;

  public PricingController(SurgeTracker surge) {
    this.surge = surge;
  }

  @GetMapping("/{city}")
  public Map<String, Object> city(@PathVariable int city) {
    List<SurgeTracker.CellSurge> cells = surge.snapshot(city);
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("cityId", city);
    out.put("maxMultiplier", surge.maxMultiplier(city));
    out.put("surgingCells", cells.stream().filter(c -> c.multiplier() > 1.0).count());
    out.put("cellMeters", surge.properties().cellMeters());
    out.put("demandWindowSeconds", surge.properties().demandWindow().toSeconds());
    out.put("cells", cells);
    return out;
  }

  @GetMapping("/{city}/quote")
  public Map<String, Object> quote(
      @PathVariable int city, @RequestParam double lat, @RequestParam double lng) {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("cityId", city);
    out.put("lat", lat);
    out.put("lng", lng);
    out.put("multiplier", surge.multiplierAt(city, lat, lng));
    return out;
  }
}
