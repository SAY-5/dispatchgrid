package io.dispatchgrid.matching;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/matching")
public class StatsController {
  private final MatchStats stats;
  private final MatchingProperties props;

  public StatsController(MatchStats stats, MatchingProperties props) {
    this.stats = stats;
    this.props = props;
  }

  @GetMapping("/stats")
  public Map<String, Object> stats() {
    return stats.snapshot();
  }

  @GetMapping("/config")
  public MatchingProperties config() {
    return props;
  }
}
